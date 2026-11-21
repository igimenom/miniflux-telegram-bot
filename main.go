package main

import (
	"embed"
	"fmt"
	"html"
	"log/slog"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/spf13/viper"
	"go.jloh.dev/miniflux-telegram-bot/models"
	"go.jloh.dev/miniflux-telegram-bot/parse"
	"go.jloh.dev/miniflux-telegram-bot/store"
	"go.jloh.dev/miniflux-telegram-bot/store/sqlite"
	"go.jloh.dev/miniflux-telegram-bot/types"
	miniflux "miniflux.app/client"
)

const (
	markRead      string = "markRead"
	markUnread    string = "markUnread"
	deleteAndMark string = "deleteAndMark"
	deleteMessage string = "deleteMessage"
	star          string = "star"
)

var (
	version = "unknown"
	commit  = "00000000"
	date    = "unknown"

	reBlockquote  = regexp.MustCompile(`(?is)<blockquote[^>]*>(.*?)</blockquote>`)
	reHTMLTags    = regexp.MustCompile(`<[^>]+>`)
	reMDHeaders   = regexp.MustCompile(`(?m)^#{1,6}\s*`)
	reBoldMD      = regexp.MustCompile(`\*\*(.+?)\*\*`)
	reIntroPrefix = regexp.MustCompile(`(?i)^(resumen en español|resumen de la noticia|resumen)\s*:?\s*`)
	reSpaces      = regexp.MustCompile(`\s+`)
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

type pendingEntry struct {
	id       int64
	firstSaw time.Time
}

type botState struct {
	mu            sync.RWMutex
	pending       []pendingEntry
	latestEntryID int64
}

func main() {
	// Get config
	viper.SetDefault("MINIFLUX_URL", "https://reader.miniflux.app")
	viper.SetDefault("MINIFLUX_SLEEP_TIME", 10)
	viper.SetDefault("TELEGRAM_CHAT_ID", 0)
	viper.SetDefault("TELEGRAM_CHANNEL_ID", int64(0))
	viper.SetDefault("TELEGRAM_POLL_TIMEOUT", 120)
	viper.SetDefault("TELEGRAM_SILENT_NOTIFICATION", true)
	viper.SetDefault("TELEGRAM_CLEANUP_MESSAGES", true)
	viper.SetDefault("TELEGRAM_SECRET", "")
	viper.SetDefault("TELEGRAM_ALLOWED_USERNAME", "")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/etc/miniflux_bot/")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		slog.Warn("Error reading in config file", "error", err)
	}

	// Pass migrations to storage
	sqlite.EmbedMigrations = embedMigrations

	// Set ChatID and optional ChannelID
	chatID := viper.GetInt64("TELEGRAM_CHAT_ID")
	if chatID == 0 {
		slog.Error("TELEGRAM_CHAT_ID is not set")
		os.Exit(1)
	}
	channelID := viper.GetInt64("TELEGRAM_CHANNEL_ID")

	// Check secret is set and valid
	telegramSecret, err := parse.TelegramSecret(viper.GetString("TELEGRAM_SECRET"))
	if err != nil {
		slog.Error("TELEGRAM_SECRET setting is invalid", "error", err)
		os.Exit(1)
	}

	// Setup RSS instance
	rss := miniflux.New(viper.GetString("MINIFLUX_URL"), viper.GetString("MINIFLUX_API_KEY"))

	// Get latest entry
	latestEntries, err := rss.Entries(&miniflux.Filter{Status: miniflux.EntryStatusUnread, Limit: 1, Direction: "desc", Order: "id"})
	if err != nil {
		slog.Error("Cannot find latest entry", "error", err)
		os.Exit(1)
	}

	// Set latest entry
	var latestEntryID int64
	if len(latestEntries.Entries) > 0 {
		latestEntryID = latestEntries.Entries[0].ID
	}

	// Get our DB going
	store := sqlite.New()

	// Initialise Telegram bot instance
	bot, err := tgbotapi.NewBotAPI(viper.GetString("TELEGRAM_BOT_TOKEN"))
	if err != nil {
		slog.Error("Failed initialising Telegram", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting Miniflux Bot", "version", version, "built", date, "commit", commit[:8], "channel_id", channelID)

	state := &botState{
		latestEntryID: latestEntryID,
	}

	// Start listening for messages from Telegram
	go listenForMessages(bot, chatID, telegramSecret, rss, store, state)

	// Cleanup & update messages
	if viper.GetBool("TELEGRAM_CLEANUP_MESSAGES") {
		go updateMessages(bot, chatID, telegramSecret, rss, store)
	}

	// Loop checking for new Miniflux entries and AI summaries
	// TODO: If webhooks get added, change over to those
	for {
		state.mu.RLock()
		currentLatestID := state.latestEntryID
		state.mu.RUnlock()

		entries, err := rss.Entries(&miniflux.Filter{Status: miniflux.EntryStatusUnread, Order: "id", AfterEntryID: currentLatestID})
		if err != nil {
			slog.Error("Failed getting entries", "error", err)
		} else if entries.Total != 0 {
			state.mu.Lock()
			for _, entry := range entries.Entries {
				if entry.ID > state.latestEntryID {
					state.latestEntryID = entry.ID
				}
				if ignoredCategoryID(entry.Feed.Category.ID) {
					slog.Info("Skipping entry as it's in an ignored category", "entry", entry.ID)
					continue
				}
				state.pending = append(state.pending, pendingEntry{
					id:       entry.ID,
					firstSaw: time.Now(),
				})
				slog.Info("Queued entry waiting for AI summary", "entry", entry.ID)
			}
			state.mu.Unlock()
		}

		// Evaluate pending entries waiting for AI summary
		state.mu.RLock()
		pendingSnapshot := make([]pendingEntry, len(state.pending))
		copy(pendingSnapshot, state.pending)
		state.mu.RUnlock()

		var stillPending []pendingEntry
		for _, item := range pendingSnapshot {
			freshEntry, err := rss.Entry(item.id)
			if err != nil {
				if strings.Contains(err.Error(), "resource not found") {
					slog.Info("Pending entry no longer exists in Miniflux, dropping from queue", "entry", item.id)
					continue
				}
				slog.Error("Failed refreshing pending entry", "error", err, "entry", item.id)
				stillPending = append(stillPending, item)
				continue
			}

			hasSummary := reBlockquote.MatchString(freshEntry.Content)
			timedOut := time.Since(item.firstSaw) >= 30*time.Minute

			if hasSummary || timedOut {
				if !hasSummary && timedOut {
					slog.Warn("Timeout waiting for AI summary, sending entry anyway", "entry", freshEntry.ID)
				}
				silent := viper.GetBool("TELEGRAM_SILENT_NOTIFICATION")
				if err := sendMsg(bot, chatID, telegramSecret, freshEntry, silent, true, store); err != nil {
					slog.Error("Failed sending message", "error", err, "entry", freshEntry.ID)
					stillPending = append(stillPending, item)
				} else {
					slog.Info("Message sent for entry", "entry", freshEntry.ID)
					if err := sendChannelMsg(bot, channelID, freshEntry, silent); err != nil {
						slog.Error("Failed sending message to channel", "error", err, "entry", freshEntry.ID, "channel", channelID)
					}
				}
			} else {
				stillPending = append(stillPending, item)
			}
		}

		state.mu.Lock()
		state.pending = stillPending
		state.mu.Unlock()

		sleepSec := viper.GetInt64("MINIFLUX_SLEEP_TIME")
		if sleepSec < 5 {
			sleepSec = 5
		}
		time.Sleep(time.Duration(sleepSec) * time.Second)
	}
}

func listenForMessages(bot *tgbotapi.BotAPI, chatID int64, secret types.TelegramSecret, rss *miniflux.Client, store store.Store, state *botState) {
	poll := tgbotapi.NewUpdate(0)
	poll.Timeout = viper.GetInt("TELEGRAM_POLL_TIMEOUT")

	updates, err := bot.GetUpdatesChan(poll)
	if err != nil {
		slog.Error("Failed getting Telegram updates", "error", err)
	}

	allowed_username := viper.GetString("TELEGRAM_ALLOWED_USERNAME")

	for update := range updates {
		// Check whether we're a command
		if update.Message != nil && update.Message.IsCommand() {
			if allowed_username != "" && update.Message.From.UserName != allowed_username {
				slog.Error("Received command from invalid user", slog.String("user", update.Message.From.UserName))
				continue
			}
			switch update.Message.Command() {
			case "randomunread":
				unreadEntries, err := rss.Entries(&miniflux.Filter{Status: miniflux.EntryStatusUnread})
				if err != nil {
					slog.Error("Failed getting unread entries from Miniflux", "error", err)
					continue
				}
				if len(unreadEntries.Entries) == 0 {
					_ = sendText(bot, chatID, "No hay noticias sin leer.", false)
					continue
				}

				if err := sendMsg(bot, chatID, secret, unreadEntries.Entries[rand.Intn(len(unreadEntries.Entries))], false, false, store); err != nil {
					slog.Error("Failed sending message for random entry", "error", err)
				}

			case "unread":
				limit := 5
				arg := strings.TrimSpace(update.Message.CommandArguments())
				if arg != "" {
					if n, err := strconv.Atoi(arg); err == nil && n > 0 {
						limit = n
						if limit > 20 {
							limit = 20
						}
					}
				}

				unreadEntries, err := rss.Entries(&miniflux.Filter{
					Status:    miniflux.EntryStatusUnread,
					Order:     "id",
					Direction: "desc",
					Limit:     limit,
				})
				if err != nil {
					slog.Error("Failed getting unread entries", "error", err)
					_ = sendText(bot, chatID, "Error al consultar Miniflux.", false)
					continue
				}
				if len(unreadEntries.Entries) == 0 {
					_ = sendText(bot, chatID, "No tienes noticias sin leer.", false)
					continue
				}

				for i := len(unreadEntries.Entries) - 1; i >= 0; i-- {
					entry := unreadEntries.Entries[i]
					if err := sendMsg(bot, chatID, secret, entry, false, true, store); err != nil {
						slog.Error("Failed sending unread entry", "error", err, "entry", entry.ID)
					}
					time.Sleep(300 * time.Millisecond)
				}

			case "resend":
				arg := strings.TrimSpace(update.Message.CommandArguments())
				entryID, err := strconv.ParseInt(arg, 10, 64)
				if err != nil || entryID <= 0 {
					_ = sendText(bot, chatID, "Uso: /resend <ID_de_entrada> (ej: /resend 642)", false)
					continue
				}

				entry, err := rss.Entry(entryID)
				if err != nil {
					slog.Error("Failed getting entry for resend", "error", err, "entry", entryID)
					_ = sendText(bot, chatID, fmt.Sprintf("No se encontró la entrada %d en Miniflux.", entryID), false)
					continue
				}

				if err := sendMsg(bot, chatID, secret, entry, false, true, store); err != nil {
					slog.Error("Failed resending entry", "error", err, "entry", entryID)
				}

			case "status":
				state.mu.RLock()
				pendingCount := len(state.pending)
				lastID := state.latestEntryID
				var oldestWait string
				if pendingCount > 0 {
					oldestWait = time.Since(state.pending[0].firstSaw).Round(time.Second).String()
				}
				state.mu.RUnlock()

				unreadStats, err := rss.Entries(&miniflux.Filter{Status: miniflux.EntryStatusUnread, Limit: 1})
				unreadTotal := -1
				if err == nil {
					unreadTotal = unreadStats.Total
				}

				var statusMsg strings.Builder
				fmt.Fprintf(&statusMsg, "Estado del bot:\n")
				fmt.Fprintf(&statusMsg, "- En cola esperando resumen IA: %d\n", pendingCount)
				if pendingCount > 0 {
					fmt.Fprintf(&statusMsg, "- Espera más antigua: %s\n", oldestWait)
				}
				fmt.Fprintf(&statusMsg, "- Último ID procesado: %d\n", lastID)
				if unreadTotal >= 0 {
					fmt.Fprintf(&statusMsg, "- Total sin leer en Miniflux: %d", unreadTotal)
				}

				if err := sendText(bot, chatID, statusMsg.String(), false); err != nil {
					slog.Error("Failed sending status message", "error", err)
				}

			case "refresh":
				if err := rss.RefreshAllFeeds(); err != nil {
					slog.Error("Failed refreshing all feeds", "error", err)
					_ = sendText(bot, chatID, "Error al solicitar la actualización de feeds en Miniflux.", false)
				} else {
					_ = sendText(bot, chatID, "Actualización de todos los feeds lanzada en Miniflux.", false)
				}

			case "search":
				query := strings.TrimSpace(update.Message.CommandArguments())
				if query == "" {
					_ = sendText(bot, chatID, "Uso: /search <texto a buscar> (ej: /search nginx)", false)
					continue
				}

				results, err := rss.Entries(&miniflux.Filter{
					Search:    query,
					Order:     "id",
					Direction: "desc",
					Limit:     5,
				})
				if err != nil {
					slog.Error("Failed searching entries", "error", err, "query", query)
					_ = sendText(bot, chatID, "Error al buscar en Miniflux.", false)
					continue
				}
				if len(results.Entries) == 0 {
					_ = sendText(bot, chatID, fmt.Sprintf("Sin resultados para \"%s\".", query), false)
					continue
				}

				_ = sendText(bot, chatID, fmt.Sprintf("Mostrando %d de %d resultados para \"%s\":", len(results.Entries), results.Total, query), true)
				for i := len(results.Entries) - 1; i >= 0; i-- {
					if err := sendMsg(bot, chatID, secret, results.Entries[i], false, false, store); err != nil {
						slog.Error("Failed sending search result", "error", err, "entry", results.Entries[i].ID)
					}
					time.Sleep(300 * time.Millisecond)
				}

			case "start":
				startMessage := fmt.Sprintf("Your Miniflux Bot is online! Running %v built %v (Commit %s)", version, date, commit[:8])
				if err = sendText(bot, chatID, startMessage, false); err != nil {
					slog.Error("Failed sending message for start command", "error", err)
				}
			}
		}

		// Check whether we've got a Callback Query
		if update.CallbackQuery != nil {
			// Double check if the callback is from our expected chat
			if update.CallbackQuery.From.ID != int(chatID) {
				slog.Warn("Callback from unexpected chat ID, ignoring", "chat_id", update.CallbackQuery.From.ID)
				continue
			}

			var entryID int64
			callback := strings.Split(update.CallbackQuery.Data, ":")

			// Check our secret
			if callback[0] != string(secret) {
				slog.Warn("Callback contained invalid secret, ignoring", "callback", callback)
				continue
			}

			// If we've got an entry, try and get it
			if len(callback) == 3 {
				entryID, err = strconv.ParseInt(callback[2], 10, 64)
				if err != nil {
					slog.Error("Failed parsing entry ID", "error", err)
				}
			}

			switch callback[1] {
			case markRead:
				if err := rss.UpdateEntries([]int64{entryID}, "read"); err != nil {
					answerCallback(bot, update.CallbackQuery.ID, "Error marking entry as read")
				} else {
					go answerCallback(bot, update.CallbackQuery.ID, "Marked entry as read")
					go updateKeyboard(bot, chatID, secret, rss, update.CallbackQuery.Message.MessageID, entryID)
					go store.UpdateEntryTime(entryID, time.Now())
				}
			case markUnread:
				if err := rss.UpdateEntries([]int64{entryID}, "unread"); err != nil {
					answerCallback(bot, update.CallbackQuery.ID, "Error marking entry as unread")
				} else {
					go answerCallback(bot, update.CallbackQuery.ID, "Marked entry as unread")
					go updateKeyboard(bot, chatID, secret, rss, update.CallbackQuery.Message.MessageID, entryID)
					go store.UpdateEntryTime(entryID, time.Now())
				}
			case deleteAndMark:
				if err := rss.UpdateEntries([]int64{entryID}, "read"); err != nil {
					answerCallback(bot, update.CallbackQuery.ID, "Error marking entry as read")
				} else {
					_, _ = bot.DeleteMessage(tgbotapi.NewDeleteMessage(chatID, update.CallbackQuery.Message.MessageID))
					_ = store.DeleteEntryByID(entryID)
					answerCallback(bot, update.CallbackQuery.ID, "Deleted message & marked as read")
				}
			case deleteMessage:
				_, _ = bot.DeleteMessage(tgbotapi.NewDeleteMessage(chatID, update.CallbackQuery.Message.MessageID))
				_ = store.DeleteEntryByTelegramID(update.CallbackQuery.Message.MessageID)
				answerCallback(bot, update.CallbackQuery.ID, "Deleted message")
			case star:
				if err := rss.ToggleBookmark(entryID); err != nil {
					answerCallback(bot, update.CallbackQuery.ID, "Error updating Miniflux entry")
					fmt.Printf("Error talking to Miniflux: %v\n", err)
				} else {
					go answerCallback(bot, update.CallbackQuery.ID, "Updated entry")
					go updateKeyboard(bot, chatID, secret, rss, update.CallbackQuery.Message.MessageID, entryID)
					go store.UpdateEntryTime(entryID, time.Now())
				}
			}
		}
	}
}

func updateMessages(bot *tgbotapi.BotAPI, chatID int64, secret types.TelegramSecret, rss *miniflux.Client, store store.Store) {
	for {
		currentTime := time.Now()

		entries, err := store.GetEntries()
		if err != nil {
			slog.Error("Failed getting saved entries", "error", err)
		}

		for _, entry := range entries {
			if currentTime.Sub(entry.SentTime).Hours() < 48 {
				minifluxEntry, err := rss.Entry(entry.ID)
				if err != nil {
					if strings.Contains(err.Error(), "resource not found") {
						slog.Info("Entry no longer exists in Miniflux, removing from store", "entry", entry.ID)
						_ = store.DeleteEntryByID(entry.ID)
					} else {
						slog.Error("Failed getting Miniflux entry", "error", err, "entry", entry.ID)
					}
					continue
				}

				if (minifluxEntry.Status == "read") && (currentTime.Sub(minifluxEntry.ChangedAt).Hours() > 2) && entry.DeleteRead {
					_, err := bot.DeleteMessage(tgbotapi.NewDeleteMessage(chatID, entry.TelegramID))
					slog.Info("Deleting message for read entry", "entry", entry.ID)
					if err != nil {
						slog.Error("Failed deleting message in Telegram", "error", err)
					}
					err = store.DeleteEntryByID(entry.ID)
					if err != nil {
						slog.Error("Error deleting entry in storage", "error", err)
					}
				} else if minifluxEntry.ChangedAt.Truncate(time.Second).After(entry.UpdatedTime) {
					slog.Info("Updating keyboard for entry", "entry", entry.ID)
					updateKeyboard(bot, chatID, secret, rss, entry.TelegramID, entry.ID)
					err := store.UpdateEntryTime(entry.ID, minifluxEntry.ChangedAt)
					if err != nil {
						slog.Error("Failed updating entry in storage", "error", err)
					}
				}
			} else {
				if err := store.DeleteEntryByID(entry.ID); err != nil {
					slog.Error("Error deleting entry in storage", "error", err)
				} else {
					slog.Info("Cleaned up old entry in storage", "entry", entry.ID)
				}
			}
		}

		time.Sleep(10 * time.Minute)
	}
}

func sendText(bot *tgbotapi.BotAPI, chatID int64, msgStr string, silentMessage bool) error {
	msg := tgbotapi.NewMessage(chatID, msgStr)
	msg.DisableNotification = silentMessage
	_, err := bot.Send(msg)
	return err
}

func buildEntryMessage(entry *miniflux.Entry) string {
	summary := extractAISummary(entry.Content)

	var sb strings.Builder
	fmt.Fprintf(&sb, "<b>%s</b>\n", html.EscapeString(entry.Title))
	fmt.Fprintf(&sb, "<i>%s in %s</i>\n", html.EscapeString(entry.Feed.Title), html.EscapeString(entry.Feed.Category.Title))

	if summary != "" {
		fmt.Fprintf(&sb, "\n%s\n", summary)
	}

	fmt.Fprintf(&sb, "\n<a href=\"%s\">%s</a>", html.EscapeString(entry.URL), html.EscapeString(entry.URL))

	msgText := sb.String()
	runes := []rune(msgText)
	if len(runes) > 4000 {
		msgText = string(runes[:4000]) + "…"
	}
	return msgText
}

func sendMsg(bot *tgbotapi.BotAPI, chatID int64, secret types.TelegramSecret, entry *miniflux.Entry, silentMessage bool, deleteRead bool, store store.Store) error {
	msg := tgbotapi.NewMessage(chatID, buildEntryMessage(entry))
	msg.ReplyMarkup = generateKeyboard(entry, secret)
	msg.ParseMode = "HTML"
	msg.DisableNotification = silentMessage
	message, err := bot.Send(msg)
	if err != nil {
		return err
	}

	var messageEntry models.Message
	messageEntry.ID = entry.ID
	messageEntry.TelegramID = message.MessageID
	messageEntry.SentTime = message.Time()
	messageEntry.UpdatedTime = entry.ChangedAt
	messageEntry.DeleteRead = deleteRead
	return store.InsertEntry(messageEntry)
}

func sendChannelMsg(bot *tgbotapi.BotAPI, channelID int64, entry *miniflux.Entry, silentMessage bool) error {
	if channelID == 0 {
		return nil
	}

	msg := tgbotapi.NewMessage(channelID, buildEntryMessage(entry))
	msg.ParseMode = "HTML"
	msg.DisableNotification = silentMessage
	_, err := bot.Send(msg)
	return err
}

func extractAISummary(content string) string {
	match := reBlockquote.FindStringSubmatch(content)
	if len(match) < 2 {
		return ""
	}

	s := html.UnescapeString(match[1])
	s = reHTMLTags.ReplaceAllString(s, " ")
	s = reMDHeaders.ReplaceAllString(s, "")
	s = reBoldMD.ReplaceAllString(s, "$1")
	s = strings.ReplaceAll(s, "###", " ")
	s = reSpaces.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	s = reIntroPrefix.ReplaceAllString(s, "")
	s = html.EscapeString(strings.TrimSpace(s))

	runes := []rune(s)
	if len(runes) > 2500 {
		s = string(runes[:2500]) + "…"
	}

	return s
}

func generateKeyboard(entry *miniflux.Entry, secret types.TelegramSecret) tgbotapi.InlineKeyboardMarkup {
	buttons := make(map[string]string)

	if entry.Starred {
		buttons["star"] = "Unstar"
	} else {
		buttons["star"] = "Star"
	}
	buttons["star_data"] = fmt.Sprintf("%s:%v:%v", secret, star, entry.ID)

	if entry.Status == "unread" {
		buttons["unread"] = "Mark as read"
		buttons["unread_data"] = fmt.Sprintf("%s:%v:%v", secret, markRead, entry.ID)
	} else {
		buttons["unread"] = "Mark as unread"
		buttons["unread_data"] = fmt.Sprintf("%s:%v:%v", secret, markUnread, entry.ID)
	}

	buttons["deleteAndMark"] = fmt.Sprintf("%s:%v:%v", secret, deleteAndMark, entry.ID)

	markup := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(buttons["unread"], buttons["unread_data"]),
			tgbotapi.NewInlineKeyboardButtonData(buttons["star"], buttons["star_data"]),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Delete message", fmt.Sprintf("%s:%v", secret, deleteMessage)),
			tgbotapi.NewInlineKeyboardButtonData("Delete & mark as read", buttons["deleteAndMark"]),
		),
	)
	return markup
}

func answerCallback(bot *tgbotapi.BotAPI, queryID string, reply string) {
	_, _ = bot.AnswerCallbackQuery(
		tgbotapi.CallbackConfig{
			CallbackQueryID: queryID,
			Text:            reply,
		},
	)
}

func updateKeyboard(bot *tgbotapi.BotAPI, chatID int64, secret types.TelegramSecret, rss *miniflux.Client, messageID int, entry int64) {
	entryData, err := rss.Entry(entry)
	if err != nil {
		slog.Error("Error updating keyboard", "error", err)
		return
	}

	msg := tgbotapi.NewEditMessageReplyMarkup(
		chatID,
		messageID,
		generateKeyboard(entryData, secret),
	)
	_, _ = bot.Send(msg)
}

func ignoredCategoryID(categoryID int64) bool {
	ignoredCategories := viper.GetStringSlice("MINIFLUX_IGNORED_CATEGORIES")
	for _, ignoredCategory := range ignoredCategories {
		if strconv.FormatInt(categoryID, 10) == ignoredCategory {
			return true
		}
	}
	return false
}