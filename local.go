package main

import (
	"github.com/opensourceways/robot-gitee-repo-watcher/models"
)

type localState struct {
	repos map[string]*models.Repo
}

var sigShaMap map[string]string
var sigOwners map[string]string
var repos map[string]string
var sigRepos map[string][]string

func (r *localState) getOrNewRepo(repo string) *models.Repo {
	if v, ok := r.repos[repo]; ok {
		return v
	}

	v := models.NewRepo(repo, models.RepoState{})
	r.repos[repo] = v

	return v
}

func (r *localState) clear(isExpectedRepo func(string) bool) {
	for k := range r.repos {
		if !isExpectedRepo(k) {
			delete(r.repos, k)
		}
	}
}

func (bot *robot) loadALLRepos(org string) (*localState, error) {
	items, err := bot.cli.GetRepos(org)
	if err != nil {
		return nil, err
	}

	r := localState{
		repos: make(map[string]*models.Repo),
	}

	for i := range items {
		item := &items[i]
		r.repos[item.Path] = models.NewRepo(item.Path, models.RepoState{
			Available: true,
			Members:   toLowerOfMembers(item.Members),
			Property: models.RepoProperty{
				Private:    item.Private,
				CanComment: item.CanComment,
			},
			Owner: item.Owner.Login,
		})
	}

	return &r, nil
}
