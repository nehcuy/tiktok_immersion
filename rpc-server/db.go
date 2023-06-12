package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
	_ "github.com/lib/pq"
)

type Database struct {
	connection *sql.DB
}

type Message struct {
	rpc.Message
}

func NewDatabase() (*Database, error) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@db:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS messages (chat TEXT, sender TEXT, text TEXT, send_time BIGINT PRIMARY KEY);")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	return &Database{
		connection: db,
	}, nil
}

func (db *Database) InsertMessage(chat string, sender string, text string, send_time int64) error {
	chat = db.ReformatChat(chat)

	insertQuery := fmt.Sprintf(
		"INSERT INTO messages (chat, sender, text, send_time) VALUES ('%s', '%s', '%s', %d)",
		chat, sender, text, send_time,
	)

	_, err := db.connection.Exec(insertQuery)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) GetMessages(chat string) ([]*Message, error) {
	chat = db.ReformatChat(chat)
	var (
		rows     *sql.Rows
		messages []*Message
		err      error
	)

	selectQuery := fmt.Sprintf(
		"SELECT * FROM messages WHERE chat='%s' ORDER BY send_time ASC", chat,
	)

	rows, err = db.connection.Query(selectQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		temp := &Message{}
		err := rows.Scan(&temp.Chat, &temp.Sender, &temp.Text, &temp.SendTime)
		if err != nil {
			return nil, err
		}
		messages = append(messages, temp)
	}
	return messages, nil
}

func (db *Database) ReformatChat(chat string) string {
	// Read the chat of message and sort it before inserting
	// For instance, {A, B} and {B, A} are the same
	chatList := strings.Split(chat, ":")
	name1 := chatList[0]
	name2 := chatList[1]
	new_chat := ""
	if name1 > name2 {
		new_chat = name2 + ":" + name1
	} else {
		new_chat = name1 + ":" + name2
	}
	return new_chat
}
