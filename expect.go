package main

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/yaml"

	"github.com/opensourceways/robot-gitee-repo-watcher/community"
)

type expectState struct {
	log *logrus.Entry
	cli iClient

	w repoBranch
	sigDir string
}

func (e *expectState) check(
	org string,
	isStopped func() bool,
	clearLocal func(func(string) bool),
	checkRepo func(*community.Repository, []string, *logrus.Entry),
) {
	newSigSha := getFirstLevelFilesSha(e)

	// refresh sigSha
	if sigShaMap[e.sigDir] == newSigSha {
		e.log.Info("there are no changes in sig directory")
	}
	sigShaMap[e.sigDir] = newSigSha

	allFiles, err := e.listAllFilesOfRepo()
	if err != nil {
		e.log.Errorf("list all file, err:%s", err.Error())

		allFiles = make(map[string]string)
	}

	// get yaml file path in allFiles but not in repos and get yaml file path which sha has changed and refresh repos
	changedRepoFilesPaths := e.getChangedRepoFilesPaths(allFiles, repos, org)
	changedSigOwnersPaths := e.getChangedOwnersFilesPaths(allFiles, sigOwners)

	if len(changedRepoFilesPaths) == 0 && len(changedSigOwnersPaths) == 0 {
		e.log.Info("yaml or OWNERS file has not been changed")
		existRepos := make(map[string]string, 0)
		items, err := e.cli.GetRepos(org)
		if err != nil {
			return
		}

		for i := range items {
			item := &items[i]
			existRepos[item.Path] = item.Name
		}

		for k := range allFiles {
			if strings.Count(k, "/") == 4 && strings.HasSuffix(k, ".yaml") {
				s := strings.Split(strings.Split(k, ".yaml")[0], "/")[4]
				if _, ok := existRepos[s]; ok {
					continue
				} else {
					delete(repos, strings.Split(k, ".yaml")[0])
					delete(community.ReposMap, strings.Split(strings.Split(k, ".yaml")[0], "/")[4])

					key := strings.Split(k, "/")[1]
					for i := 0; i < len(sigRepos[key]); i++ {
						if sigRepos[key][i] == s {
							sigRepos[key] = append(sigRepos[key][:i], sigRepos[key][i+1:]...)
							i--
						}
					}
				}
			}
		}
	}

	if len(changedRepoFilesPaths) > 0 {
		for k := range changedRepoFilesPaths {
			c, err := e.loadFile(k)
			if err != nil {
				e.log.Warning("can't load file")
				continue
			}

			v := &community.Repository{}
			err = decodeYamlFile(c, v)
			if err != nil {
				e.log.Warning("can't decode file")
				continue
			}
			community.ReposMap[v.Name] = v
		}
	}

	if len(changedSigOwnersPaths) > 0 {
		for k := range changedSigOwnersPaths {
			if strings.HasSuffix(k, "OWNERS") {
				c, err := e.loadFile(k)
				if err != nil {
					e.log.Warning("can't load file")
				}

				v := &community.RepoOwners{}
				err = decodeYamlFile(c, v)
				if err != nil {
					e.log.Warning("can't decode file")
				}
				community.ReposOwners[strings.Split(k, "/")[1]] = v
			}
		}
	}

	repoMap := community.ReposMap

	if len(repoMap) == 0 {
		// keep safe to do this. it is impossible to happen generally.
		e.log.Warning("there are not repos. Impossible!!!")
		return
	}

	clearLocal(func(r string) bool {
		_, ok := repoMap[r]
		return ok
	})
	done := sets.NewString()

	for sigName, v := range sigRepos {
		owners := toLowerOfMembers(community.ReposOwners[sigName].Maintainers)

		for _, repoName := range v {
			if isStopped() {
				break
			}

			if org == "repo-watch" && repoName == "blog" {
				continue
			}

			checkRepo(community.ReposMap[repoName], owners, e.log)

			done.Insert(repoName)
		}

		if isStopped() {
			break
		}
	}

	if len(repoMap) == done.Len() {
		return
	}

	for k, repo := range repoMap {
		if isStopped() {
			break
		}

		if !done.Has(k) {
			if org == "openeuler" && k == "blog" {
				continue
			}

			checkRepo(repo, nil, e.log)
		}
	}
}

func (e *expectState) listAllFilesOfRepo() (map[string]string, error) {
	trees, err := e.cli.GetDirectoryTree(e.w.Org, e.w.Repo, e.w.Branch, 1)
	if err != nil || len(trees.Tree) == 0 {
		return nil, err
	}

	r := make(map[string]string)
	for i := range trees.Tree {
		item := &trees.Tree[i]
		r[item.Path] = item.Sha
	}

	return r, nil
}

func (e *expectState) loadFile(f string) (string, error) {
	c, err := e.cli.GetPathContent(e.w.Org, e.w.Repo, f, e.w.Branch)
	if err != nil {
		return "", err
	}

	return c.Content, nil
}

func decodeYamlFile(content string, v interface{}) error {
	c, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(c, v)
}

func (e *expectState) getChangedRepoFilesPaths(allFiles, compareFiles map[string]string, org string) map[string]string {
	r := make(map[string]string, 0)

	e.removeDeletedRepos(allFiles, org)

	for k, v := range allFiles {
		if strings.HasPrefix(k, e.sigDir) && !strings.HasSuffix(k, ".md") &&
			strings.Count(k, "/") == 4 && strings.Split(k, "/")[2] == org {
			if _, ok := compareFiles[strings.Split(k, ".yaml")[0]]; ok {
				if compareFiles[strings.Split(k, ".yaml")[0]] != v {
					r[k] = v
					compareFiles[strings.Split(k, ".yaml")[0]] = v
				}
			} else {
				r[k] = v
				compareFiles[strings.Split(k, ".yaml")[0]] = v
			}

			if v, ok := sigRepos[strings.Split(k, "/")[1]]; ok {
				in := findInSlice(v, strings.Split(strings.Split(k, "/")[4], ".yaml")[0])
				if !in {
					sigRepos[strings.Split(k, "/")[1]] =
						append(sigRepos[strings.Split(k, "/")[1]], strings.Split(strings.Split(k, "/")[4], ".yaml")[0])
				}
			} else {
				sigRepos[strings.Split(k, "/")[1]] = []string{strings.Split(strings.Split(k, "/")[4], ".yaml")[0]}
			}
		}
	}

	return r
}

func (e *expectState) getChangedOwnersFilesPaths(allFiles, compareFiles map[string]string) map[string]string {

	r := make(map[string]string, 0)

	for k, v := range allFiles {
		if strings.HasPrefix(k, e.sigDir) && strings.HasSuffix(k, "OWNERS") {
			if _, ok := compareFiles[k]; ok {
				if compareFiles[k] != v {
					r[k] = v
					compareFiles[k] = v
				}
			} else {
				r[k] = v
				compareFiles[k] = v
			}
		}
	}

	return r
}

func findInSlice(s []string, k string) (in bool) {

	in = false
	for _, j := range s {
		if j == k {
			in = true
		} else {
			continue
		}
	}

	return in
}

func (e *expectState) removeDeletedRepos(allFiles map[string]string, org string) {
	l := make(map[string][]string, 0)
	for k := range allFiles {
		if strings.HasPrefix(k, e.sigDir) && !strings.HasSuffix(k, ".md") &&
			strings.Count(k, "/") == 4 && strings.Contains(k, org) {
			l[strings.Split(k, "/")[1]] = append(l[strings.Split(k, "/")[1]], strings.Split(strings.Split(k, "/")[4], ".yaml")[0])
		}
	}
	for k, v := range l {
		o := sigRepos[k]
		newO := sets.NewString(v...)
		oldO := sets.NewString(o...)

		if needToDelete := oldO.Difference(newO); needToDelete.Len() > 0 {
			for i := range needToDelete {
				delete(community.ReposMap, i)
				delete(repos, fmt.Sprintf("%s/%s/%s/%s/%s", e.sigDir, k, org, strings.ToLower(i[:1]), i))

				for j := 0; j < len(sigRepos[k]); j++ {
					if sigRepos[k][j] == i {
						sigRepos[k] = append(sigRepos[k][:j], sigRepos[k][j+1:]...)
						j--
					}
				}
			}
		}

	}
}

func getFirstLevelFilesSha(e *expectState) string {
	tree, err := e.cli.GetDirectoryTree(e.w.Org, e.w.Repo, e.w.Branch, 0)
	if err != nil || len(tree.Tree) == 0 {
		e.log.Errorf("list all file, err:%s", err)

		return ""
	}

	for i := range tree.Tree {
		item := &tree.Tree[i]
		if item.Path == e.sigDir {
			return item.Sha
		}
	}

	return ""
}
