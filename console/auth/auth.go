package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"

	"util/log"
	"net/http"
	"strings"
	"time"
	"console/config"
)

var authConfig *AuthConfig

type UserInfo struct {
	UserId   uint64 `json:"userId"`
	UserName string `json:"username"`
	Expire   int64
}
type SsoReply struct {
	ReqCode int32     `json:"REQ_CODE"`
	ReqData *UserInfo `json:"REQ_DATA"`
	ReqFlag bool      `json:"REQ_FLAG"`
	ReqMsg  string    `json:"REQ_MSG"`
}
type AuthConfig struct {
	cache       map[string]*UserInfo
	loginConfig *config.LoginConfig
	client      *http.Client
	cacheTime   time.Duration
}

func initLoginConfig(cfg *config.Config) *config.LoginConfig {
	return &config.LoginConfig{
		SsoLoginUrl:    cfg.SsoLoginUrl,
		SsoLogoutUrl:   cfg.SsoLogoutUrl,
		SsoCookieName:  cfg.SsoCookieName,
		SsoDomainName:  cfg.SsoDomainName,
		SsoExcludePath: cfg.SsoExcludePath,
		SsoVerifyUrl:   cfg.SsoVerifyUrl,
		AppDomainName:  cfg.AppDomainName,
		AppUrl:         cfg.AppUrl,
		AppName:        cfg.AppName,
		AppToken:       cfg.AppToken,
	}
}

func Author(cfg *config.Config) gin.HandlerFunc {
	authConfig = &AuthConfig{
		loginConfig: initLoginConfig(cfg),
		cache:       make(map[string]*UserInfo),
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
		cacheTime: 30 * time.Minute,
	}

	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		// authority only when path is not being skipped
		if ok := isExclude(path); !ok {
			session := sessions.Default(c)
			userName, ok := session.Get("user_name").(string)
			if !ok {
				//从缓存获取用户信息
				userInfo := getUserInfoFromCache("admin")
				if userInfo == nil {
					clientIP := c.ClientIP()
					log.Debug("get user info from remote, user:[admin], ip:[%v], path:[%v]", clientIP, path)
					//远程获取用户信息
					userInfo, _ = getUserInfoFromRemote("admin", path, clientIP)
					//更新到缓存
					saveUserInfoToCache("admin", userInfo)
				}
				userName = userInfo.UserName
				log.Debug("start to update session, user info: [%v]", userInfo)
				session.Set("user_name", userInfo.UserName)
				session.Save()
			}
			end := time.Now()
			latency := end.Sub(start)
			log.Debug("user[%v] login take time : [%v]", userName, latency)
		}
		// Process request
		c.Next()
	}
}

func getUserInfoFromCache(name string) *UserInfo {
	if authConfig.cache == nil {
		return nil
	}
	userInfo, found := authConfig.cache[name]
	if !found {
		log.Debug("can't found user info from cache by name[%v]", name)
		return nil
	}
	for _, c := range authConfig.cache {
		log.Debug("user cache is: [%v]", c)
	}
	if userInfo.Expire < time.Now().Unix() {
		delete(authConfig.cache, name)
		return nil
	}
	return userInfo
}

func saveUserInfoToCache(name string, userInfo *UserInfo) {
	expireTime := time.Now().Add(authConfig.cacheTime)
	log.Debug("set expire time is:[%v] ", expireTime)
	userInfo.Expire = expireTime.Unix()
	authConfig.cache[name] = userInfo
}

func getUserInfoFromRemote(name, uri, clintIp string) (*UserInfo, error) {
	userInfo := &UserInfo{
		UserId:   1,
		UserName: "admin",
		Expire:   10 * 365 * 24 * 3600,
	}
	return userInfo, nil
}

func isExclude(uri string) bool {
	if authConfig.loginConfig == nil || authConfig.loginConfig.SsoExcludePath == nil || len(authConfig.loginConfig.SsoExcludePath) == 0 {
		log.Debug("no exclude path cache")
		return false
	}
	for _, path := range authConfig.loginConfig.SsoExcludePath {
		if strings.HasPrefix(uri, path){
			return true
		}
	}
	return false
}
