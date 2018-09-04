package right

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"
	"github.com/muesli/cache2go"
	_ "github.com/go-sql-driver/mysql"
	"util/log"
	"database/sql"
	"time"
)

var UserCache = cache2go.Cache("user_cache")

const (
	CLUSTER_USER = 0x4
	CLUSTER_ADMIN = 0x2
	SYSTEM_ADMIN = 0x1
)
type Right int16
type User struct {
	Name string
	Right map[int64]Right // clusterid: right
}

func NewUser(name string) *User{
	return &User{
		Name: name,
		Right: make(map[int64]Right),
	}
}

func (user *User) addClusterRight(id, right int64) {
	user.Right[id] = Right(right)
}

func AddCacheUser(user *User) {
	UserCache.Add(user.Name, 5*time.Minute, user)
}

func DelCacheUser(userName string) {
	UserCache.Delete(userName)
}

func GetCacheUser(userName string) *User {
	res, err := UserCache.Value(userName)
	if err != nil {
		log.Error("user [%v] not cached", userName)
		return nil
	}
	return res.Data().(*User)
}

func GetPrivilege(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer c.Next()

		userName, ok := sessions.Default(c).Get("user_name").(string)
		if !ok {
			log.Error("user type assert failed")
			return
		}
		log.Debug("user [%v] get privilege", userName)

		if _, err := GetUserCluster(db, userName); err != nil {
			log.Error("user_name [%v] is not exist: %v", userName, err)
			return
		}
	}
}

func GetUserCluster(db *sql.DB, userName string) (*User, error) {
	return getUserClusterFake(db, userName)
}

func getUserClusterFake(db *sql.DB, userName string) (*User, error) {
	user := &User{
		Name: userName,
		Right: map[int64]Right{0:1},
	}
	AddCacheUser(user)
	return user, nil
}
