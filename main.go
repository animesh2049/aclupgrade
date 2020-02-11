package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/dgraph-io/dgo/x"
	"google.golang.org/grpc"
)

type Groups []Group
type Rules []Rule

type Group struct {
	uid   int    `json:"uid"`
	rules string `json:"dgraph.group.acl"`
}

type Rule struct {
	predicate  string `json:"predicate"`
	permission int    `json:"perm"`
}

func main() {
	alpha := flag.String("alpha", "localhost:9180", "Alpha end point")
	userName := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	flag.Parse()

	conn, err := grpc.Dial(*alpha, grpc.WithInsecure())
	x.Check(err)
	defer conn.Close()

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	err = dg.Login(ctx, *userName, *password)
	x.Check(err)

	query := `
	{
		me(func: type(Group)) {
			uid
			dgraph.group.acl
		}
	}
	`

	ctx, _ = context.WithTimeout(ctx, 5*time.Second)
	resp, err := dg.NewReadOnlyTxn().Query(ctx, query)
	x.Check(err)

	var buf bytes.Buffer
	data := make(map[string]Groups)
	err = json.Unmarshal(resp.GetJson(), &data)
	x.Check(err)
	groups, ok := data["me"]
	if !ok {
		fmt.Errorf("Unable to parse ACLs: %+v", string(resp.GetJson()))
		os.Exit(1)
	}

	for _, group := range groups {
		var rules Rules
		ruleCount := 1
		fmt.Println(group)
		err = json.Unmarshal([]byte(group.rules), &rules)
		x.Check(err)
		for _, rule := range rules {
			newRule := fmt.Sprintf(`%s <dgraph.acl.rules> _:newrule%[2]d\n_:newrule%[2]d <dgraph.rule.predicate> %s\n_:newrule%[2]d <dgraph.rule.permission> %d`, group.uid, ruleCount, rule.predicate, rule.permission)
			buf.WriteString(newRule)
			ruleCount++
		}
	}

	fmt.Println(buf.String())
}
