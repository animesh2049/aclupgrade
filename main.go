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

type Rules []Rule

type Group struct {
	Uid   string `json:"uid"`
	Rules string `json:"dgraph.group.acl,omitempty"`
}

type Rule struct {
	Predicate  string `json:"predicate,omitempty"`
	Permission int    `json:"perm,omitempty"`
}

func main() {
	alpha := flag.String("alpha", "localhost:9180", "Alpha end point")
	userName := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	outFile := flag.String("output", "acl_rules.rdf", "Write output to a file instead of stdout")
	flag.Parse()

	if _, err := os.Stat(*outFile); err == nil {
		fmt.Println("Output file already exists.")
		os.Exit(1)
	}

	f, err := os.OpenFile(*outFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Errorf("Error writing output file %s", outFile)
		os.Exit(1)
	}

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
	data := make(map[string][]Group)
	err = json.Unmarshal(resp.GetJson(), &data)
	x.Check(err)

	groups, ok := data["me"]
	if !ok {
		fmt.Errorf("Unable to parse ACLs: %+v", string(resp.GetJson()))
		os.Exit(1)
	}

	ruleString := `<%s> <dgraph.acl.rule> _:newrule%[2]d .
_:newrule%[2]d <dgraph.rule.predicate> "%s" .
_:newrule%[2]d <dgraph.rule.permission> "%[4]d" .
`

	ruleCount := 1
	for _, group := range groups {
		var rules Rules
		if group.Rules == "" {
			continue
		}

		err = json.Unmarshal([]byte(group.Rules), &rules)
		x.Check(err)
		for _, rule := range rules {
			newRule := fmt.Sprintf(ruleString, group.Uid, ruleCount,
				rule.Predicate, rule.Permission)
			buf.WriteString(newRule)
			ruleCount++
		}
	}

	fmt.Fprintln(f, buf.String())
}
