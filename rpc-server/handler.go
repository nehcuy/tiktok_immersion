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
	resp := rpc.NewSendResponse()
	resp.SetCode(0)
	resp.SetMsg("success")

	chat := req.GetMessage().GetChat()
	sender := req.GetMessage().GetSender()
	text := req.GetMessage().GetText()
	time := req.GetMessage().GetSendTime()

	fmt.Println(prettyPrint(chat, sender, text, time))

	err := s.db.InsertMessage(chat, sender, text, time)
	if err != nil {
		resp.SetCode(1)
		resp.SetMsg(err.Error())
	}

	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	chat := req.GetChat()
	messages, err := s.db.GetMessages(chat)
	if err != nil {
		return nil, err
	}

	respMessages := make([]*rpc.Message, 0)
	for _, message := range messages {
		temp := &rpc.Message{
			Chat:     s.db.ReformatChat(chat),
			Sender:   message.Sender,
			Text:     message.Text,
			SendTime: message.SendTime,
		}
		respMessages = append(respMessages, temp)
		fmt.Println(prettyPrint(chat, message.Sender, message.Text, message.SendTime))
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
	// split
	return "{CHAT: " + chat + ";\nSENDER: " + sender + ";\nCONTENT: " + text + ";\nTIME: " + pretty_time + "}"
}
