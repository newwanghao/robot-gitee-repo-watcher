package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/robot-gitee-repo-watcher/community"
	"github.com/opensourceways/robot-gitee-repo-watcher/models"
)

type expectRepoInfo struct {
	expectRepoState *community.Repository
	expectOwners    []string
	org             string
}

func (e *expectRepoInfo) getNewRepoName() string {
	return e.expectRepoState.Name
}

func (bot *robot) run(ctx context.Context, log *logrus.Entry) error {
	w := &bot.cfg.WatchingFiles
	expect := &expectState{
		w:   w.repoBranch,
		log: log,
		cli: bot.cli,
		sigDir: w.SigDir,
	}

	err := bot.initSigShaAndAllReposFiles(w)
	if err != nil {
		log.Errorf("list all files failed: %v", err)
	}

	err = expect.loadFileToLocal()
	if err != nil {
		return err
	}

	org := w.RepoFilePath

	local, err := bot.loadALLRepos(org)
	if err != nil {
		return err
	}

	bot.watch(ctx, org, local, expect)
	return nil
}

func (bot *robot) watch(ctx context.Context, org string, local *localState, expect *expectState) {
	if interval := bot.cfg.Interval; interval <= 0 {
		for {
			if isCancelled(ctx) {
				break
			}

			bot.checkOnce(ctx, org, local, expect)
		}
	} else {
		t := time.Duration(interval) * time.Minute

		for {
			if isCancelled(ctx) {
				break
			}

			s := time.Now()

			bot.checkOnce(ctx, org, local, expect)

			e := time.Now()
			if v := e.Sub(s); v < t {
				time.Sleep(t - v)
			}
		}
	}

	bot.wg.Wait()
}

func (bot *robot) checkOnce(ctx context.Context, org string, local *localState, expect *expectState) {
	f := func(repo *community.Repository, owners []string, log *logrus.Entry) {
		if repo == nil {
			return
		}

		err := bot.execTask(
			local.getOrNewRepo(repo.Name),
			expectRepoInfo{
				org:             org,
				expectOwners:    owners,
				expectRepoState: repo,
			},
			log,
		)
		if err != nil {
			log.Errorf("submit task of repo:%s, err:%s", repo.Name, err.Error())
		}
	}

	isStopped := func() bool {
		return isCancelled(ctx)
	}

	expect.log.Info("new check")

	expect.check(org, isStopped, local.clear, f)
}

func (bot *robot) execTask(localRepo *models.Repo, expectRepo expectRepoInfo, log *logrus.Entry) error {
	f := func(before models.RepoState) models.RepoState {
		if !before.Available {
			return bot.createRepo(expectRepo, log, bot.createOBSMetaProject)
		}

		return models.RepoState{
			Available: true,
			Branches:  bot.handleBranch(expectRepo, before.Branches, log),
			Members:   bot.handleMember(expectRepo, before.Members, &before.Owner, log),
			Property:  bot.updateRepo(expectRepo, before.Property, log),
			Owner:     before.Owner,
		}
	}

	bot.wg.Add(1)
	err := bot.pool.Submit(func() {
		defer bot.wg.Done()

		localRepo.Update(f)
	})
	if err != nil {
		bot.wg.Done()
	}
	return err
}

func isCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// 初始化仓库的文件路径
func (bot *robot) initSigShaAndAllReposFiles(w *watchingFiles) error {
	trees, err := bot.cli.GetDirectoryTree(w.Org, w.Repo, w.Branch, 1)

	if err != nil || len(trees.Tree) == 0 {
		return err
	}

	repos = make(map[string]string)
	sigOwners = make(map[string]string)
	sigShaMap = make(map[string]string)
	sigRepos = make(map[string][]string)

	for i := range trees.Tree {
		item := &trees.Tree[i]
		if strings.HasPrefix(item.Path, w.SigDir) && strings.Count(item.Path, "/") == 4 &&
			strings.Split(item.Path, "/")[2] == w.RepoFilePath{
			repos[strings.Split(item.Path, ".yaml")[0]] = item.Sha
			sigRepos[strings.Split(item.Path, "/")[1]] =
				append(sigRepos[strings.Split(item.Path, "/")[1]], strings.Split(strings.Split(item.Path, "/")[4], ".yaml")[0])
		} else if item.Path == w.SigDir {
			sigShaMap[item.Path] = item.Sha
		} else if strings.HasSuffix(item.Path, "OWNERS") {
			sigOwners[item.Path] = item.Sha
		}
	}

	return nil
}

func (e *expectState) loadFileToLocal() error {
	t := time.Now()
	community.ReposMap = make(map[string]*community.Repository)
	for k := range repos {
		c, err := e.loadFile(fmt.Sprintf("%s.yaml", k))
		if err != nil {
			return err
		}

		v := &community.Repository{}
		err = decodeYamlFile(c, v)
		if err != nil {
			return err
		}

		community.ReposMap[v.Name] = v
	}

	community.ReposOwners = make(map[string]*community.RepoOwners)
	for j := range sigOwners {
		c, err := e.loadFile(j)
		if err != nil {
			return err
		}

		v := &community.RepoOwners{}
		err = decodeYamlFile(c, v)
		if err != nil {
			return err
		}

		community.ReposOwners[strings.Split(j, "/")[1]] = v
	}

	return nil
}
