package main

import (
	"context"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/kitex_gen/rpc"
	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/kitex_gen/rpc/imservice"
	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/proto_gen/api"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/kitex/client"
	etcd "github.com/kitex-contrib/registry-etcd"
)

var cli imservice.Client

func main() {
	r, err := etcd.NewEtcdResolver([]string{"etcd:2379"})
	if err != nil {
		log.Fatal(err)
	}
	cli = imservice.MustNewClient("demo.rpc.server",
		client.WithResolver(r),
		client.WithRPCTimeout(1*time.Second),
		client.WithHostPorts("rpc-server:8888"),
	)

	h := server.Default(server.WithHostPorts("0.0.0.0:8080"))

	h.GET("/ping", func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"message": "pong"})
	})

	h.POST("/api/send", sendMessage)
	h.GET("/api/pull", pullMessage)

	h.Spin()
}

func sendMessage(ctx context.Context, c *app.RequestContext) {
	var req api.SendRequest
	err := c.Bind(&req)
	if err != nil {
		c.String(consts.StatusBadRequest, "Failed to parse request body: %v", err)
		return
	}

	sender, has_sender := c.GetQuery("sender")
	receiver, has_receiver := c.GetQuery("receiver")
	chat := sender + ":" + receiver
	if !has_sender || !has_receiver {
		c.String(consts.StatusBadRequest, "Sender and Receiver fields must not be empty")
		return
	}

	text, has_text := c.GetQuery("text")
	if !has_text {
		c.String(consts.StatusBadRequest, "Text field must not be empty")
		return
	}

	resp, err := cli.Send(ctx, &rpc.SendRequest{
		Message: &rpc.Message{
			Chat:     chat,
			Text:     text,
			Sender:   sender,
			SendTime: time.Now().UnixNano(),
		},
	})

	if err != nil {
		c.String(consts.StatusInternalServerError, err.Error())
	} else if resp.Code != 0 {
		c.String(consts.StatusInternalServerError, resp.Msg)
	} else {
		c.String(consts.StatusOK, resp.Msg)
	}
}

func pullMessage(ctx context.Context, c *app.RequestContext) {
	var req api.PullRequest
	err := c.Bind(&req)
	if err != nil {
		c.String(consts.StatusBadRequest, "Failed to parse request body: %v", err)
		return
	}

	chat, has_chat := c.GetQuery("chat")
	if !has_chat {
		c.String(consts.StatusBadRequest, "Chat field must not be empty")
		return
	}
	pattern := `^[\w]+:[\w]+$`
	successful_chat_query := regexp.MustCompile(pattern).MatchString(chat)
	if !successful_chat_query {
		c.String(consts.StatusBadRequest, "Chat field must be in the format of '<name1>:<name2>' of the chat between the 2 people.")
		return
	}

	cur, has_cursor := c.GetQuery("cursor")
	if !has_cursor {
		// if field not specified, default is 0
		cur = "0"
	}
	cursor, err := strconv.ParseInt(cur, 10, 64)
	if err != nil {
		c.String(consts.StatusBadRequest, "Cursor field must be an integer of type int64")
		return
	}

	lim, has_limit := c.GetQuery("limit")
	if !has_limit {
		// if field no specified, default is 10
		lim = "10"
	}
	limit, err := strconv.ParseInt(lim, 10, 32)
	if err != nil {
		c.String(consts.StatusBadRequest, "Limit field must be an integer of type int32")
		return
	}

	rev, is_reverse := c.GetQuery("reverse")
	if !is_reverse {
		// if field not specified, default is false
		rev = "false"
	}

	reverse, err := strconv.ParseBool(rev)
	if err != nil {
		c.String(consts.StatusBadRequest, "Reverse field must be a boolean")
		return
	}

	resp, err := cli.Pull(ctx, &rpc.PullRequest{
		Chat:    chat,
		Cursor:  cursor,
		Limit:   int32(limit),
		Reverse: &reverse,
	})

	if err != nil {
		c.String(consts.StatusInternalServerError, err.Error())
		return
	} else if resp.Code != 0 {
		c.String(consts.StatusInternalServerError, resp.Msg)
		return
	}
	messages := make([]*api.Message, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		messages = append(messages, &api.Message{
			Chat:     msg.Chat,
			Text:     msg.Text,
			Sender:   msg.Sender,
			SendTime: msg.SendTime,
		})
	}
	c.JSON(consts.StatusOK, &api.PullResponse{
		Messages:   messages,
		HasMore:    resp.GetHasMore(),
		NextCursor: resp.GetNextCursor(),
	})
}
