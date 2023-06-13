package main

import (
	"context"
	"fmt"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct {
	db *Database
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	message := req.GetMessage()
	resp := rpc.NewSendResponse()

	err := s.db.InsertMessage(message)
	if err != nil {
		return nil, err
	}

	chat := message.GetChat()
	sender := message.GetSender()
	text := message.GetText()
	time := message.GetSendTime()

	fmt.Println(prettyPrint(chat, sender, text, time))

	resp.SetCode(0)
	resp.SetMsg("success")
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	messages, err := s.db.GetMessages(req)
	if err != nil {
		return nil, err
	}

	respMessages := make([]*rpc.Message, 0)
	for _, message := range messages {
		chat := req.GetChat()
		sender := message.GetSender()
		text := message.GetText()
		send_time := message.GetSendTime()

		temp := &rpc.Message{
			Chat:     chat,
			Sender:   sender,
			Text:     text,
			SendTime: send_time,
		}
		respMessages = append(respMessages, temp)

		fmt.Println(prettyPrint(chat, sender, text, send_time))
	}
	resp := rpc.NewPullResponse()
	resp.SetCode(0)
	resp.SetMsg("success")
	resp.SetMessages(respMessages)

	return resp, nil
}

func prettyPrint(chat string, sender string, text string, send_time int64) string {
	t := time.Unix(send_time, 0)
	pretty_time := t.Format("2006-01-02 15:04:05")
	return "{CHAT: " + chat + "; SENDER: " + sender + "; CONTENT: " + text + "; TIME: " + pretty_time + "}"
}
