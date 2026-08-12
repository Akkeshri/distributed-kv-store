package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	baseURL string
	explain bool
)

func main() {
	root := &cobra.Command{
		Use:   "kvctl",
		Short: "CLI for distributed KV store",
	}

	root.PersistentFlags().StringVar(&baseURL, "url", "http://127.0.0.1:8080", "coordinator node URL")
	root.PersistentFlags().BoolVar(&explain, "explain", false, "include explain metadata")

	root.AddCommand(setCmd(), getCmd(), deleteCmd(), rangeCmd(), clusterStateCmd(), nodeFailCmd(), nodeRecoverCmd(), startClusterCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func setCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set [key] [value]",
		Short: "Set a key",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			body, _ := json.Marshal(map[string]string{"value": args[1]})
			url := fmt.Sprintf("%s/kv/%s", strings.TrimRight(baseURL, "/"), args[0])
			if explain {
				url += "?explain=true"
			}
			return doJSON(http.MethodPut, url, body)
		},
	}
}

func getCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get [key]",
		Short: "Get a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("%s/kv/%s", strings.TrimRight(baseURL, "/"), args[0])
			if explain {
				url += "?explain=true"
			}
			return doJSON(http.MethodGet, url, nil)
		},
	}
}

func deleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete [key]",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("%s/kv/%s", strings.TrimRight(baseURL, "/"), args[0])
			if explain {
				url += "?explain=true"
			}
			return doJSON(http.MethodDelete, url, nil)
		},
	}
}

func rangeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "range",
		Short: "Range scan on local node store",
		RunE: func(cmd *cobra.Command, args []string) error {
			start, _ := cmd.Flags().GetString("start")
			end, _ := cmd.Flags().GetString("end")
			url := fmt.Sprintf("%s/kv/range?start=%s&end=%s", strings.TrimRight(baseURL, "/"), start, end)
			return doJSON(http.MethodGet, url, nil)
		},
	}
	cmd.Flags().String("start", "", "start key")
	cmd.Flags().String("end", "", "end key")
	return cmd
}

func clusterStateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cluster state",
		Short: "Show cluster state",
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("%s/cluster/state", strings.TrimRight(baseURL, "/"))
			return doJSON(http.MethodGet, url, nil)
		},
	}
}

func nodeFailCmd() *cobra.Command {
	var nodeURL string
	cmd := &cobra.Command{
		Use:   "node fail [node-id]",
		Short: "Simulate node failure via admin API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			url := nodeURL
			if url == "" {
				url = strings.TrimRight(baseURL, "/")
			}
			return doJSON(http.MethodPost, url+"/admin/fail", nil)
		},
	}
	cmd.Flags().StringVar(&nodeURL, "node-url", "", "direct URL of node to fail (defaults to --url)")
	return cmd
}

func nodeRecoverCmd() *cobra.Command {
	var nodeURL string
	cmd := &cobra.Command{
		Use:   "node recover [node-id]",
		Short: "Simulate node recovery via admin API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			url := nodeURL
			if url == "" {
				url = strings.TrimRight(baseURL, "/")
			}
			return doJSON(http.MethodPost, url+"/admin/recover", nil)
		},
	}
	cmd.Flags().StringVar(&nodeURL, "node-url", "", "direct URL of node to recover (defaults to --url)")
	return cmd
}

func startClusterCmd() *cobra.Command {
	var nodes, replication, readQ, writeQ int
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a local multi-node cluster (in-process helper)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("Use: go run ./cmd/kvcluster start --nodes 3")
			return nil
		},
	}
	cmd.Flags().IntVar(&nodes, "nodes", 3, "number of nodes")
	cmd.Flags().IntVar(&replication, "replication", 3, "replication factor")
	cmd.Flags().IntVar(&readQ, "read-quorum", 2, "read quorum")
	cmd.Flags().IntVar(&writeQ, "write-quorum", 2, "write quorum")
	return cmd
}

func doJSON(method, url string, body []byte) error {
	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewReader(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		fmt.Println(string(data))
	} else {
		fmt.Println(pretty.String())
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("request failed: %s", resp.Status)
	}
	return nil
}
