package main

import (
	"flag"
	"fmt"
	"io"
	"model/pkg/ds_admin"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"

	_ "model/pkg/ds_admin"
	client "pkg-go/ds_client"
	"util/log"
)

var token = flag.String("token", "", "auth token")
var addr = flag.String("addr", "127.0.0.1:16180", "dataserver admin addr")

var cli client.AdminClient

var completer = readline.NewPrefixCompleter(
	readline.PcItem("config",
		readline.PcItem("get"),
		readline.PcItem("set"),
	),
	readline.PcItem("info"),
	readline.PcItem("split",
		readline.PcItem("<range id>"),
	),
	readline.PcItem("compaction"),
	readline.PcItem("clearq",
		readline.PcItem(""),
		readline.PcItem("all"),
		readline.PcItem("slow"),
		readline.PcItem("fast"),
	),
	readline.PcItem("flush"),
)

func usage(w io.Writer) {
	io.WriteString(w, "commands:\n")
	io.WriteString(w, completer.Tree("    "))
	io.WriteString(w, "\n")
}

func main() {
	flag.Parse()

	log.SetLevel("ERROR")
	cli = client.NewAdminClient(*token, 1)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            fmt.Sprintf("%s> ", *addr),
		AutoComplete:      completer,
		HistoryFile:       filepath.Join(os.TempDir(), fmt.Sprintf("readline.tmp.%d", time.Now().Unix())),
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, "config set "):
			setCfg(cli, strings.TrimSpace(line[11:]))
		case strings.HasPrefix(line, "config get "):
			getCfg(cli, strings.TrimSpace(line[11:]))
		case strings.HasPrefix(line, "info"):
			resp, err := cli.GetInfo(*addr, strings.TrimSpace(line[4:]))
			if err != nil {
				println("ERR: ", err.Error())
			} else {
				println(resp.Data)
			}
		case strings.HasPrefix(line, "split "):
			split(cli, strings.TrimSpace(line[6:]))
		case strings.HasPrefix(line, "compaction"):
			compaction(cli, line[10:])
		case strings.HasPrefix(line, "clearq"):
			clearQ(cli, strings.TrimSpace(line[6:]))
		case strings.HasPrefix(line, "flush"):
			flush(cli, strings.TrimSpace(line[5:]))

		case line == "help":
			fallthrough
		case line == "h":
			usage(rl.Stderr())
		default:
			println("ERR: unsupported command:", strconv.Quote(line))
		}
	}
}

func parseCfgKey(arg string) (*ds_adminpb.ConfigKey, bool) {
	keys := strings.Split(arg, ".")
	if len(keys) < 1 {
		return nil, false
	} else if len(keys) == 1 {
		return &ds_adminpb.ConfigKey{
			Name: keys[0],
		}, true
	} else {
		return &ds_adminpb.ConfigKey{
			Section: keys[0],
			Name:    keys[1],
		}, true
	}
}

func setCfg(cli client.AdminClient, arg string) {
	args := strings.Split(arg, " ")
	if len(args) != 2 {
		println("invalid args", arg)
		return
	}
	key, ret := parseCfgKey(args[0])
	if !ret {
		println("ERR: invalid config key: ", arg[0])
		return
	}
	err := cli.SetConfig(*addr, []*ds_adminpb.ConfigItem{
		&ds_adminpb.ConfigItem{
			Key:   key,
			Value: args[1],
		},
	})
	if err != nil {
		println("ERR: ", err.Error())
	} else {
		fmt.Printf("Set %s.%s to %s successfully.\n", key.Section, key.Name, args[1])
	}
}

func getCfg(cli client.AdminClient, arg string) {
	key, ret := parseCfgKey(arg)
	if !ret {
		println("ERR: invalid config key: ", arg)
		return
	}
	resp, err := cli.GetConfig(*addr, []*ds_adminpb.ConfigKey{key})
	if err != nil {
		println("ERR: ", err.Error())
	} else {
		for _, cfg := range resp.Configs {
			println(cfg.String())
		}
	}
}

func compaction(cli client.AdminClient, arg string) {
	var rangeID uint64
	var err error
	if len(arg) > 0 {
		rangeID, err = strconv.ParseUint(arg, 10, 64)
		if err != nil {
			println("ERR: invalid range id: ", arg)
			return
		}
	}
	resp, err := cli.ForceCompact(*addr, rangeID, 0)
	if err != nil {
		println("ERR: ", err.Error())
	} else {
		fmt.Printf("Compact range %v-%v successfully.\n", resp.GetBeginKey(), resp.GetEndKey())
	}
}

func split(cli client.AdminClient, arg string) {
	rangeID, err := strconv.ParseUint(arg, 10, 64)
	if err != nil {
		println("ERR: invalid range id: ", arg)
		return
	}
	err = cli.ForceSplit(*addr, rangeID, 0)
	if err != nil {
		fmt.Printf("ERR: request split range[%d] failed: %s\n", rangeID, err.Error())
	} else {
		fmt.Printf("request split range[%d] successfully", rangeID)
	}
}

func clearQ(cli client.AdminClient, arg string) {
	typ := ds_adminpb.ClearQueueRequest_ALL
	switch arg {
	case "slow":
		typ = ds_adminpb.ClearQueueRequest_SLOW_WORKER
	case "fast":
		typ = ds_adminpb.ClearQueueRequest_FAST_WORKER
	}
	resp, err := cli.ClearQueue(*addr, typ)
	if err != nil {
		println("ERR: ", err.Error())
	} else {
		fmt.Printf("total %s cleared: %d\n", typ.String(), resp.GetCleared())
	}
}

func flush(cli client.AdminClient, arg string) {
	wait := false
	if arg == "1" || arg == "wait" {
		wait = true
	}
	err := cli.FlushDB(*addr, wait)
	if err != nil {
		println("ERR: ", err.Error())
	} else {
		fmt.Printf("flush successully. wait=%v\n", wait)
	}
}
