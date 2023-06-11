package main

import (
	"context"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct {
	db *Database
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	resp := rpc.NewSendResponse()
	resp.SetCode(0)
	chat := "{CHAT: " + req.GetMessage().GetChat() + "; "
	sender := "SENDER: " + req.GetMessage().GetSender() + "; "
	text := "CONTENT: " + req.GetMessage().GetText() + "}"
	prettyPrint := chat + sender + text
	resp.SetMsg(prettyPrint)
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	return resp, nil
}
