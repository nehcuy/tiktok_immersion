package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Database struct {
	connection *sql.DB
}

func NewDatabase() (*Database, error) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@db:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS messages (uuid UUID PRIMARY KEY, chat TEXT, sender TEXT, text TEXT, send_time BIGINT);")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	return &Database{
		connection: db,
	}, nil
}

func (db *Database) InsertMessage(message *rpc.Message) error {
	uuid := uuid.New().String()
	chat := db.ReformatChat(message.GetChat())
	sender := message.GetSender()
	text := message.GetText()
	send_time := message.GetSendTime()

	insertQuery := fmt.Sprintf(
		"INSERT INTO messages (uuid, chat, sender, text, send_time) VALUES ('%s', '%s', '%s', '%s', %d)",
		uuid, chat, sender, text, send_time,
	)

	_, err := db.connection.Exec(insertQuery)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) GetMessages(req *rpc.PullRequest) ([]*rpc.Message, error) {
	var (
		rows     *sql.Rows
		messages []*rpc.Message
		err      error
	)

	chat := db.ReformatChat(req.GetChat())
	cursor := req.GetCursor()
	limit := req.GetLimit()
	is_reverse := req.GetReverse()
	reverse := ""
	if !is_reverse {
		reverse = "ASC"
	} else {
		reverse = "DESC"
	}

	selectQuery := fmt.Sprintf(
		"SELECT chat, sender, text, send_time FROM messages WHERE chat = '%s' AND send_time >= %d ORDER BY send_time %s LIMIT %d",
		chat, cursor, reverse, limit,
	)

	fmt.Println(selectQuery)

	rows, err = db.connection.Query(selectQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		temp := &rpc.Message{}
		err := rows.Scan(&temp.Chat, &temp.Sender, &temp.Text, &temp.SendTime)
		if err != nil {
			return nil, err
		}
		messages = append(messages, temp)
	}
	return messages, nil
}

/*
 *	Reorder the chat to standardise storage order.
 *	For instance, {A, B} and {B, A} are the same.
 */
func (db *Database) ReformatChat(chat string) string {
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
