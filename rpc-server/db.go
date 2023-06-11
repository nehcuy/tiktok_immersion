package main

import (
	"database/sql"
	"log"
	"strings"
	"fmt"

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

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS messages (chat TEXT PRIMARY KEY, sender TEXT, text TEXT, send_time BIGINT);")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	return &Database{
		connection: db,
	}, nil
}

func (db *Database) InsertMessage(chat string, sender string, text string, send_time int64) error {
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
	
	insertQuery := fmt.Sprintf(
		"INSERT INTO messages (chat, sender, text, send_time) VALUES ('%s', '%s', '%s', %d)",
		new_chat, sender, text, send_time,
	)

	_, err := db.connection.Exec(insertQuery)
	if err != nil {
		return err
	}
	return nil
}
