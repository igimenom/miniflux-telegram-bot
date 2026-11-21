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

	reBlockquote = regexp.MustCompile(`(?is)<blockquote[^>]*>(.*?)</blockquote>`)
	reBoldMD     = regexp.MustCompile(`\*\*(.+?)\*\*`)
	reHTMLTags   = regexp.MustCompile(`<[^>]+>`)
	reSpaces     = regexp.MustCompile(`[ \t]{2,}`)
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

type pendingEntry struct {
	id       int64
	firstSaw time.Time
}

func main() {
	// Get config
	viper.SetDefault("MINIFLUX_URL", "https://reader.miniflux.app")
	viper.SetDefault("MINIFLUX_SLEEP_TIME", 10)
	viper.SetDefault("TELEGRAM_CHAT_ID", 0)
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

	// Set ChatID
	chatID := viper.GetInt64("TELEGRAM_CHAT_ID")
	if chatID == 0 {
		slog.Error("TELEGRAM_CHAT_ID is not set")
		os.Exit(1)
	}

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

	slog.Info("Starting Miniflux Bot", "version", version, "built", date, "commit", commit[:8])

	// Start listening for messages from Telegram
	go listenForMessages(bot, chatID, telegramSecret, rss, store)

	// Cleanup & update messages
	if viper.GetBool("TELEGRAM_CLEANUP_MESSAGES") {
		go updateMessages(bot, chatID, telegramSecret, rss, store)
	}

	var pending []pendingEntry

	// Loop checking for new Miniflux entries and AI summaries
	// TODO: If webhooks get added, change over to those
	for {
		entries, err := rss.Entries(&miniflux.Filter{Status: miniflux.EntryStatusUnread, Order: "id", AfterEntryID: latestEntryID})
		if err != nil {
			slog.Error("Failed getting entries", "error", err)
		} else if entries.Total != 0 {
			for _, entry := range entries.Entries {
				latestEntryID = entry.ID
				if ignoredCategoryID(entry.Feed.Category.ID) {
					slog.Info("Skipping entry as it's in an ignored category", "entry", entry.ID)
					continue
				}
				pending = append(pending, pendingEntry{
					id:       entry.ID,
					firstSaw: time.Now(),
				})
				slog.Info("Queued entry waiting for AI summary", "entry", entry.ID)
			}
		}

		// Evaluate pending entries waiting for AI summary
		var stillPending []pendingEntry
		for _, item := range pending {
			freshEntry, err := rss.Entry(item.id)
			if err != nil {
				slog.Error("Failed refreshing pending entry", "error", err, "entry", item.id)
				stillPending = append(stillPending, item)
				continue
			}

			hasSummary := reBlockquote.MatchString(freshEntry.Content)
			timedOut := time.Since(item.firstSaw) >= 10*time.Minute

			if hasSummary || timedOut {
				if !hasSummary && timedOut {
					slog.Warn("Timeout waiting for AI summary, sending entry anyway", "entry", freshEntry.ID)
				}
				if err := sendMsg(bot, chatID, telegramSecret, freshEntry, viper.GetBool("TELEGRAM_SILENT_NOTIFICATION"), true, store); err != nil {
					slog.Error("Failed sending message", "error", err, "entry", freshEntry.ID)
					stillPending = append(stillPending, item)
				} else {
					slog.Info("Message sent for entry", "entry", freshEntry.ID)
				}
			} else {
				stillPending = append(stillPending, item)
			}
		}
		pending = stillPending

		sleepSec := viper.GetInt64("MINIFLUX_SLEEP_TIME")
		if sleepSec < 5 {
			sleepSec = 5
		}
		time.Sleep(time.Duration(sleepSec) * time.Second)
	}
}

func listenForMessages(bot *tgbotapi.BotAPI, chatID int64, secret types.TelegramSecret, rss *miniflux.Client, store store.Store) {
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
					_ = sendText(bot, chatID, "No unread entries found.", false)
					continue
				}

				// Select a random entry from the list
				if err := sendMsg(bot, chatID, secret, unreadEntries.Entries[rand.Intn(len(unreadEntries.Entries))], false, false, store); err != nil {
					slog.Error("Failed sending message for random entry", "error", err)
				}
			case "start":
				startMessage := fmt.Sprintf("Your Miniflux Bot is online! Running %v built %v (Commit %s)", version, date, commit[:8])
				if err = sendText(bot, chatID, startMessage, false); err != nil {
					slog.Error("Failed sending message for start command", "error", err)
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

				// Enviar de más antigua a más reciente dentro del lote
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
			// Split our string
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
		// Set current time so we know when messages are too old to remove
		currentTime := time.Now()

		// Get all our entries
		entries, err := store.GetEntries()
		if err != nil {
			slog.Error("Failed getting saved entries", "error", err)
		}

		for _, entry := range entries {
			if currentTime.Sub(entry.SentTime).Hours() < 48 {

				// We can edit the message!
				minifluxEntry, err := rss.Entry(entry.ID)
				if err != nil {
					slog.Error("Failed getting Miniflux entry", "error", err)
					continue
				}

				// If we're read, were updated > 2 hours ago and set to delete it, cleanup the message
				if (minifluxEntry.Status == "read") && (currentTime.Sub(minifluxEntry.ChangedAt).Hours() > 2) && entry.DeleteRead {
					_, err := bot.DeleteMessage(tgbotapi.NewDeleteMessage(chatID, entry.TelegramID))
					slog.Info("Deleting message for read entry", "entry", entry.ID)
					if err != nil {
						slog.Error("Failed deleting message in Telegram", "error", err)
					}
					// Cleanup entry in DB
					err = store.DeleteEntryByID(entry.ID)
					if err != nil {
						slog.Error("Error deleting entry in storage", "error", err)
					}
				} else if minifluxEntry.ChangedAt.Truncate(time.Second).After(entry.UpdatedTime) {
					// If entry has been updated in Miniflux (marked as read, starred etc) update Telegram keyboard
					slog.Info("Updating keyboard for entry", "entry", entry.ID)
					updateKeyboard(bot, chatID, secret, rss, entry.TelegramID, entry.ID)
					err := store.UpdateEntryTime(entry.ID, minifluxEntry.ChangedAt)
					if err != nil {
						slog.Error("Failed updating entry in storage", "error", err)
					}
				}
			} else {
				// Cleanup the DB entry since there is nothing we can do with it
				if err := store.DeleteEntryByID(entry.ID); err != nil {
					slog.Error("Error deleting entry in storage", "error", err)
				} else {
					slog.Info("Cleaned up old entry in storage", "entry", entry.ID)
				}
			}
		}

		// Sleep for 10 minutes until we check again
		time.Sleep(10 * time.Minute)
	}
}

func sendText(bot *tgbotapi.BotAPI, chatID int64, msgStr string, silentMessage bool) error {
	msg := tgbotapi.NewMessage(chatID, msgStr)
	msg.DisableNotification = silentMessage
	_, err := bot.Send(msg)
	return err
}

func sendMsg(bot *tgbotapi.BotAPI, chatID int64, secret types.TelegramSecret, entry *miniflux.Entry, silentMessage bool, deleteRead bool, store store.Store) error {
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

	msg := tgbotapi.NewMessage(chatID, msgText)
	msg.ReplyMarkup = generateKeyboard(entry, secret)
	msg.ParseMode = "HTML"
	msg.DisableNotification = silentMessage
	message, err := bot.Send(msg)
	if err != nil {
		return err
	}

	// Save our message
	var messageEntry models.Message
	messageEntry.ID = entry.ID
	messageEntry.TelegramID = message.MessageID
	messageEntry.SentTime = message.Time()
	messageEntry.UpdatedTime = entry.ChangedAt
	messageEntry.DeleteRead = deleteRead
	if err := store.InsertEntry(messageEntry); err != nil {
		return err
	}

	return nil
}

func extractAISummary(content string) string {
	match := reBlockquote.FindStringSubmatch(content)
	if len(match) < 2 {
		return ""
	}

	// Decodifica entidades HTML como &#34; -> "
	s := html.UnescapeString(match[1])

	// Quita etiquetas HTML existentes (p, strong, etc.) dejando espacios limpios
	s = reHTMLTags.ReplaceAllString(s, " ")

	// Escapa caracteres especiales para HTML de Telegram
	s = html.EscapeString(s)

	// Convierte **negrita** en <b>negrita</b>
	s = reBoldMD.ReplaceAllString(s, "<b>$1</b>")

	// Limpia espacios múltiples y recorta
	s = reSpaces.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)

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
	// Get latest entry data
	entryData, err := rss.Entry(entry)
	if err != nil {
		slog.Error("Error updating keyboard", "error", err)
		return
	}

	// Generate new keyboard data
	msg := tgbotapi.NewEditMessageReplyMarkup(
		chatID,
		messageID,
		generateKeyboard(entryData, secret),
	)
	_, _ = bot.Send(msg)
}

// Check whether the CategoryID is in our ignored list
func ignoredCategoryID(categoryID int64) bool {
	ignoredCategories := viper.GetStringSlice("MINIFLUX_IGNORED_CATEGORIES")
	for _, ignoredCategory := range ignoredCategories {
		if strconv.FormatInt(categoryID, 10) == ignoredCategory {
			return true
		}
	}
	return false
}