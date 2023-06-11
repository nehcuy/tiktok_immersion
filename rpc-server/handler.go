package main

import (
	"context"
	"strconv"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct {
	db *Database
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	resp := rpc.NewSendResponse()
	resp.SetCode(0)
	chat := req.GetMessage().GetChat()
	sender := req.GetMessage().GetSender()
	text := req.GetMessage().GetText()
	time := req.GetMessage().GetSendTime()
	resp.SetMsg(prettyPrint(chat, sender, text, time))

	err := s.db.InsertMessage(chat, sender, text, time)
	if err != nil {
		resp.SetCode(1)
		resp.SetMsg(err.Error())
	}

	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	return resp, nil
}

func prettyPrint(chat string, sender string, text string, send_time int64) string {
	return "{CHAT: " + chat + "; SENDER: " + sender + "; CONTENT: " + text + "; TIME: " + strconv.Itoa(int(send_time)) + "}"
}
