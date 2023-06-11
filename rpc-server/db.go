package main

import (
	"database/sql"
	"log"

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
	// placeholder username, password, database_name
	db, err := sql.Open("postgres", "postgres://user:password@localhost/database_name?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	return &Database{
		connection: db,
	}, nil
}
