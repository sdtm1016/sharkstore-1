/**
 * 请求路由统一定义
 */
package routers

import (
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"github.com/gin-contrib/sessions"

	"path/filepath"
	"net/http"
	"database/sql"
	"fmt"

	"console/controllers"
	"console/common"
	"console/config"
	"console/auth"
	"console/right"
	"util/log"
)

type Router struct {
	config        *config.Config
	viewsRootDir  string
	staticRootDir string
	db            *sql.DB
}

func NewRouter(c *config.Config, db *sql.DB) *Router {
	r := new(Router)
	r.config = c
	r.viewsRootDir = filepath.Join(c.ProjectHomeDir, "/views/*")
	r.staticRootDir = filepath.Join(c.ProjectHomeDir, "/static")
	r.db = db

	return r
}

func (r *Router) GetUserCluster(userName string) (*right.User, error) {
	return right.GetUserCluster(r.db, userName)
}

func (r *Router) StartRouter() *gin.Engine {
	router := gin.New()
	store := sessions.NewCookieStore([]byte("jDIkFg6ju7kEM7DOIWGcXSLwCL6QaMZy"))
	store.Options(sessions.Options{
		Path:     "/",
		HttpOnly: false,
		Secure:   false,
		MaxAge:   3600,
	})
	router.Use(sessions.Sessions("session_id", store))
	router.Use(auth.Author(r.config))
	router.Use(gin.Logger(), gin.Recovery())
	// because gin default delimiter {{}} conflict to AngularJs, so change delimiter
	router.Delims("{[{", "}]}")
	router.LoadHTMLGlob(r.viewsRootDir)
	router.Static("/static", r.staticRootDir)

	router.GET("/", func(c *gin.Context) {
		userName, ok := sessions.Default(c).Get("user_name").(string)
		if !ok {
			c.Redirect(http.StatusMovedPermanently, "/logout")
		}
		admin := "none"
		user, err := r.GetUserCluster(userName)
		if err == nil {
			for _, r := range user.Right {
				if r == 1 {
					admin = ""
					break
				}
			}
		}
		c.HTML(http.StatusOK, "index.html", gin.H{
			"basePath": r.staticRootDir,
			"userName": userName,
			"admin":    admin,
		})
	})

	router.GET("/logout", func(c *gin.Context) {
		session := sessions.Default(c)
		session.Clear()
		session.Save()
		c.Redirect(http.StatusMovedPermanently, r.config.SsoLogoutUrl+"?ReturnUrl="+r.config.AppUrl)
	})

	// -----------------page controller router -------------

	router.GET("/page/user/welcome", func(c *gin.Context) {
		c.HTML(http.StatusOK, "user_welcome.html", gin.H{
			"basePath": r.staticRootDir,
		})
	})

	router.GET("/page/sql/viewApplyList", func(c *gin.Context) {
		userName, ok := sessions.Default(c).Get("user_name").(string)
		if !ok {
			c.Redirect(http.StatusMovedPermanently, "/logout")
		}
		admin := "none"
		user, err := r.GetUserCluster(userName)
		if err == nil {
			for _, r := range user.Right {
				if r == 1 {
					admin = ""
					break
				}
			}
		}
		c.HTML(http.StatusOK, "sqlapply_list.html", gin.H{
			"basePath": r.staticRootDir,
			"admin":    admin,
		})
	})

	router.GET("/page/lock/viewNamespace", func(c *gin.Context) {
		userName, ok := sessions.Default(c).Get("user_name").(string)
		if !ok {
			c.Redirect(http.StatusMovedPermanently, "/logout")
		}
		admin := "none"
		user, err := r.GetUserCluster(userName)
		if err == nil {
			for _, r := range user.Right {
				if r == 1 {
					admin = ""
					break
				}
			}
		}
		c.HTML(http.StatusOK, "locknsp_list.html", gin.H{
			"basePath": r.staticRootDir,
			"admin":    admin,
		})
	})

	router.GET("/page/lock/applyNamespace", func(c *gin.Context) {
		c.HTML(http.StatusOK, "locknsp_apply.html", gin.H{
			"basePath": r.staticRootDir,
		})
	})

	router.GET("/page/lock/viewLock", func(c *gin.Context) {
		cid := c.Query("clusterId")
		dbName := c.Query("dbName")
		tableName := c.Query("tableName")
		if cid == "" || dbName == "" || tableName == "" {
			html404(c)
			return
		}

		c.HTML(http.StatusOK, "lock_list.html", gin.H{
			"basePath":  r.staticRootDir,
			"clusterId": cid,
			"dbName":    dbName,
			"tableName": tableName,
		})
	})

	router.GET("/page/configure/viewNamespace", func(c *gin.Context) {
		userName, ok := sessions.Default(c).Get("user_name").(string)
		if !ok {
			c.Redirect(http.StatusMovedPermanently, "/logout")
		}
		admin := "none"
		user, err := r.GetUserCluster(userName)
		if err == nil {
			for _, r := range user.Right {
				if r == 1 {
					admin = ""
					break
				}
			}
		}
		c.HTML(http.StatusOK, "configurensp_list.html", gin.H{
			"basePath": r.staticRootDir,
			"admin":    admin,
		})
	})

	router.GET("/page/configure/viewList", func(c *gin.Context) {
		cid := c.Query("clusterId")
		dbName := c.Query("dbName")
		tableName := c.Query("tableName")
		if cid == "" || dbName == "" || tableName == "" {
			html404(c)
			return
		}

		c.HTML(http.StatusOK, "configure_list.html", gin.H{
			"basePath":  r.staticRootDir,
			"clusterId": cid,
			"dbName":    dbName,
			"tableName": tableName,
		})
	})

	group := router.Group("/", right.GetPrivilege(r.db))
	{
		group.GET("/page/cluster/viewCluster", func(c *gin.Context) {
			c.HTML(http.StatusOK, "cluster_list.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		group.GET("/page/cluster/info", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				log.Debug("param clusterId not exists")
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "cluster_info.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})

		group.GET("/page/cluster/source", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "cluster_source.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})

		group.GET("/page/cluster/createcluster", func(c *gin.Context) {
			c.HTML(http.StatusOK, "cluster_createcluster.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		group.GET("/page/master/viewNodeList", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "master_info.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})

		router.GET("/userInfo/viewUserInfoList", func(c *gin.Context) {
			c.HTML(http.StatusOK, "userinfo.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})
		router.GET("/userInfo/viewPrivilegeList", func(c *gin.Context) {
			c.HTML(http.StatusOK, "userprivilege.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})
		router.GET("/userInfo/viewRoleInfoList", func(c *gin.Context) {
			c.HTML(http.StatusOK, "roleinfo.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		group.GET("/page/metadata/metadatacreatedb", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "metadata_metadatacreatedb.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})

		group.GET("/page/metadata/metadatacreatetable", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "metadata_metadatacreatetable.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})
		group.GET("/page/metadata/createtable", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbId := c.Query("dbId")
			dbName := c.Query("dbName")

			c.HTML(http.StatusOK, "metadata_createtable.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"dbId":      dbId,
				"dbName":    dbName,
			})
		})
		group.GET("/page/cluster/topology", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "cluster_topologylist.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})
		group.GET("/page/tables/tablelist", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbId := c.Query("dbId")
			dbName := c.Query("dbName")

			userName, ok := sessions.Default(c).Get("user_name").(string)
			if !ok {
				c.Redirect(http.StatusMovedPermanently, "/logout")
			}
			admin := "none"
			user, err := r.GetUserCluster(userName)
			if err == nil {
				for _, r := range user.Right {
					if r == 1 {
						admin = ""
						break
					}
				}
			}
			c.HTML(http.StatusOK, "tables_tablelist.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"dbId":      dbId,
				"dbName":    dbName,
				"admin":     admin,
			})
		})
		group.GET("/page/table/topology", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}
			tableName := c.Query("tableName")
			if tableName == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "table_topologymiss.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"dbName":    dbName,
				"tableName": tableName,
			})
		})
		group.GET("/page/table/duplicateRange", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}
			tableName := c.Query("tableName")
			if tableName == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "table_rangedup.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"dbName":    dbName,
				"tableName": tableName,
			})
		})
		group.GET("/page/metadata/edittable", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			tableName := c.Query("tableName")
			if tableName == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "metadata_edittable.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"tableName": tableName,
				"dbName":    dbName,
			})
		})
		group.GET("/page/metadata/viewTableColumns", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			tableName := c.Query("name")
			if tableName == "" {
				html404(c)
				return
			}
			dbName := c.Query("db_name")
			if dbName == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "metadata_viewtablecolumn.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"name":      tableName,
				"dbName":    dbName,
			})
		})

		group.GET("/page/db/viewConsole", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}
			dbId := c.Query("dbId")
			if dbName == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "db_consoleview.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"dbName":    dbName,
				"dbId":      dbId,
			})
		})

		group.GET("/range/viewRangeInfo", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			tableName := c.Query("name")
			if tableName == "" {
				html404(c)
				return
			}
			dbName := c.Query("db_name")
			if dbName == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "range_viewrangevis.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"rangeId":   "",
				"tableName": tableName,
				"dbName":    dbName,
				"source":    "table",
			})
		})

		group.GET("/range/getRangeTopo", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			rangeId := c.Query("rangeId")
			if rangeId == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "range_viewrangevis.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"rangeId":   rangeId,
				"tableName": "",
				"dbName":    "",
				"source":    "range",
			})
		})

		group.GET("/node/getRangeTopo", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			nodeId := c.Query("nodeId")
			if nodeId == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "range_viewrangevis.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"rangeId":   nodeId,
				"tableName": "",
				"dbName":    "",
				"source":    "node",
			})
		})

		group.GET("/node/goConfigPage", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			nodeId := c.Query("nodeId")
			if nodeId == "" {
				html404(c)
				return
			}

			isGet := c.Query("isGet")
			if isGet == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "node_get_set_config.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"nodeId":   nodeId,
				"isGet":     isGet,
			})
		})

		group.GET("/page/cluster/viewRangeOpsTopN", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			topN := c.Query("topN")
			if topN == "" {
				html404(c)
				return
			}

			c.HTML(http.StatusOK, "range_viewopstopn.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"topN":      topN,
			})
		})

		group.GET("/page/range/unhealthy", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}
			tableName := c.Query("tableName")
			if tableName == "" {
				html404(c)
				return
			}
			rId := c.Query("rangeId")
			c.HTML(http.StatusOK, "range_unhealthy.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"tableName": tableName,
				"dbName":    dbName,
				"rangeId":   rId,
			})
		})

		group.GET("/page/range/unstable", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}
			tableName := c.Query("tableName")
			if tableName == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "range_unstable.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"tableName": tableName,
				"dbName":    dbName,
			})
		})

		group.GET("/page/range/peerinfo", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dbName := c.Query("dbName")
			if dbName == "" {
				html404(c)
				return
			}
			tableName := c.Query("tableName")
			if tableName == "" {
				html404(c)
				return
			}
			rangeId := c.Query("rangeId")
			if rangeId == "" {
				html404(c)
				return
			}
			flag := c.Query("flag")
			c.HTML(http.StatusOK, "range_peerinfo.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
				"tableName": tableName,
				"dbName":    dbName,
				"rangeId":   rangeId,
				"flag":      flag,
			})
		})

		router.GET("/page/system/initCluster", func(c *gin.Context) {
			c.HTML(http.StatusOK, "cluster_init.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		router.GET("/page/system/scheduleManage", func(c *gin.Context) {
			c.HTML(http.StatusOK, "schedule_manage.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		router.GET("/page/system/scheduleAdjust", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			c.HTML(http.StatusOK, "schedule_adjust.html", gin.H{
				"basePath":  r.staticRootDir,
				"clusterId": cid,
			})
		})

		group.GET("/page/monitor/cluster", func(c *gin.Context) {
			cid := c.Query("clusterId")
			if cid == "" {
				html404(c)
				return
			}
			dashboardName := c.Query("dashboardName")
			if dashboardName == "" {
				html404(c)
				return
			}
			panelId := c.Query("panelId")
			if panelId == "" {
				html404(c)
				return
			}
			if r.config.MonitorDomain == "" {
				html404(c)
				return
			}
			var startTime = c.Query("startTime")
			var endTime = c.Query("endTime")
			typeQ := c.Query("type")
			if typeQ == "" {
				c.Redirect(http.StatusMovedPermanently, fmt.Sprintf("%s/dashboard-solo/db/%s?var-cluster_id=%s&panelId=%s&from=%s&to=%s", r.config.MonitorDomain, dashboardName, cid, panelId, startTime, endTime))
			} else {
				c.Redirect(http.StatusMovedPermanently, fmt.Sprintf("%s/dashboard-solo/db/%s?var-cluster_id=%s&panelId=%s&from=%s&to=%s&var-type=%s", r.config.MonitorDomain, dashboardName, cid, panelId, startTime, endTime, typeQ))
			}
		})

		router.GET("/page/metric/viewServerList", func(c *gin.Context) {
			c.HTML(http.StatusOK, "metric_server.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		router.GET("/page/metric/clusterManage", func(c *gin.Context) {
			c.HTML(http.StatusOK, "metric_manage.html", gin.H{
				"basePath": r.staticRootDir,
			})
		})

		// ----------------api router ---------------------
		// cluster
		router.GET(controllers.REQURI_CLUSTER_GETALL, func(c *gin.Context) {
			handleRightAndAction(r, c, controllers.NewClusterGetAllAction())
		})
		router.POST(controllers.REQURI_CLUSTER_GETBYID, func(c *gin.Context) {
			handleAction(c, controllers.NewClusterGetByIdAction())
		})
		router.POST(controllers.REQURI_CLUSTER_CREATE, func(c *gin.Context) {
			handleAction(c, controllers.NewClusterCreateAction())
		})
		router.POST(controllers.REQURI_CLUSTER_INIT, func(c *gin.Context) {
			handleAction(c, controllers.NewClusterInitAction())
		})
		router.POST(controllers.REQURI_CLUSTER_TOGGLEAUTO, func(c *gin.Context) {
			handleAction(c, controllers.NewClusterToggleAction())
		})

		//master
		router.POST(controllers.REQURI_MASTER_All, func(c *gin.Context) {
			handleAction(c, controllers.NewMasterAllAction())
		})
		router.POST(controllers.REQURI_MASTER_LEADER, func(c *gin.Context) {
			handleAction(c, controllers.NewMasterLeaderAction())
		})
		router.POST(controllers.REQURI_MASTER_LOGLEVEL_UPDATE, func(c *gin.Context) {
			handleAction(c, controllers.NewMasterLogLevelUpdate())
		})
		// metadata
		router.POST(controllers.REQURL_META_CREATEDB, func(c *gin.Context) {
			handleAction(c, controllers.NewCreateDbAction())
		})
		router.POST(controllers.REQURL_META_DELETEDB, func(c *gin.Context) {
			handleAction(c, controllers.NewDeleteDbAction())
		})
		router.POST(controllers.REQURL_META_GETALLDB, func(c *gin.Context) {
			handleAction(c, controllers.NewGetAllDbAction())
		})
		// TODO：不要与页面绑定，精简掉
		router.GET(controllers.REQURL_META_GETALLDBVIEW, func(c *gin.Context) {
			if data, err := controllers.NewGetAllDbViewAction().Execute(c); err != nil {
				log.Error("getalldbview action error. err:[%v]", err)
			} else {
				c.JSON(http.StatusOK, data)
			}
		})
		router.POST(controllers.REQURL_META_CREATETABLE, func(c *gin.Context) {
			handleAction(c, controllers.NewCreateTableAction())
		})
		router.GET(controllers.REQURL_META_GETALLTABLE, func(c *gin.Context) {
			handleAction(c, controllers.NewGetAllTableAction())
		})
		router.POST(controllers.REQURL_META_DELTABLE, func(c *gin.Context) {
			handleAction(c, controllers.NewDeleteTableAction())
		})
		router.POST(controllers.REQURL_META_EDITTABLE, func(c *gin.Context) {
			handleAction(c, controllers.NewEditTableAction())
		})
		router.POST(controllers.REQURL_META_TABLERWPOLICY, func(c *gin.Context) {
			handleAction(c, controllers.NewEditTablePolicy())
		})
		router.POST(controllers.REQURL_META_GETTABLECOLUMNS, func(c *gin.Context) {
			handleAction(c, controllers.NewGetTableColumns())
		})
		router.GET(controllers.RANGE_GETRANGEBYDBTABLE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeViewInfo())
		})
		router.POST(controllers.NODE_NODEINFOGETALL, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeViewInfo())
		})
		router.POST(controllers.NODE_NODESTATUSUPDATE, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeStatusUpdate())
		})
		router.POST(controllers.NODE_NODELOGLEVELUPDATE, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeLogLevelUpdate())
		})
		router.POST(controllers.NODE_DELETENODE, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeDelete())
		})
		router.GET(controllers.NODE_GET_RANGE_TOPOLOGY, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeRangeTopo())
		})
		router.POST(controllers.NODE_UPDATE_ISOLATION_LABEL, func(c *gin.Context) {
			handleAction(c, controllers.NewUpdateIsolationLabel())
		})
		router.POST(controllers.NODE_GET_CONFIG, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeGetConfig())
		})
		router.POST(controllers.NODE_SET_CONFIG, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeSetConfig())
		})
		router.POST(controllers.NODE_GET_DS_INFO, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeGetDsInfo())
		})
		router.POST(controllers.NODE_CLEAR_QUEUE, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeClearQueue())
		})
		router.POST(controllers.NODE_GET_PENDING_QUEUES, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeGetPendingQueues())
		})
		router.POST(controllers.NODE_FLUSH_DB, func(c *gin.Context) {
			handleAction(c, controllers.NewNodeFlushDB())
		})
		router.POST(controllers.RANGE_PEERDEL, func(c *gin.Context) {
			handleAction(c, controllers.NewPeerDelete())
		})
		router.POST(controllers.RANGE_PEERADD, func(c *gin.Context) {
			handleAction(c, controllers.NewPeerAdd())
		})
		router.GET(controllers.RANGE_GET_UNHEALTHY_RANGES, func(c *gin.Context) {
			handleAction(c, controllers.NewGetUnhealthyRanges())
		})
		router.GET(controllers.RANGE_GET_UNSTABLE_RANGES, func(c *gin.Context) {
			handleAction(c, controllers.NewGetUnstableRanges())
		})
		router.GET(controllers.RANGE_GET_RANGE_INFO_BY_ID, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeInfoView())
		})
		router.GET(controllers.RANGE_GET_PEER_INFO, func(c *gin.Context) {
			handleAction(c, controllers.NewPeerInfoView())
		})
		router.POST(controllers.RANGE_UPDATE_RANGE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeUpdate())
		})
		router.POST(controllers.RANGE_OFFLINE_RANGE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeOffline())
		})
		router.POST(controllers.RANGE_REBUILD_RANGE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeRebuild())
		})
		router.POST(controllers.RANGE_REPLACE_RANGE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeRebuild())
		})
		router.POST(controllers.RANGE_FORCE_SPLIT, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeForceSplit())
		})
		router.POST(controllers.RANGE_FORCE_COMPACT, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeForceCompact())
		})
		router.POST(controllers.RANGE_DELETE_RANGE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeDelete())
		})
		router.GET(controllers.RANGE_GET_TOPOLOGY, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeTopo())
		})
		router.POST(controllers.RANGE_BATCH_RECOVER_RANGE, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeBatchRecover())
		})
		router.POST(controllers.RANGE_TRANSFER, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeTransfer())
		})
		router.POST(controllers.RANGE_CHANGE_LEADER, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeLeaderChange())
		})
		router.GET(controllers.RANGE_OPS_TOPN, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeOpsTopN())
		})
		router.POST(controllers.TASK_GET_PRESENT, func(c *gin.Context) {
			handleAction(c, controllers.NewTaskPresent())
		})
		router.POST(controllers.TASK_OPERATION, func(c *gin.Context) {
			handleAction(c, controllers.NewTaskOperation())
		})
		router.GET(controllers.REQURI_SCHEDULER_GETALL, func(c *gin.Context) {
			handleAction(c, controllers.NewSchedulerAllAction())
		})
		router.GET(controllers.REQURI_SCHEDULER_GETDETAIL, func(c *gin.Context) {
			handleAction(c, controllers.NewSchedulerDetailAction())
		})
		router.POST(controllers.REQURI_SCHEDULER_ADJUST, func(c *gin.Context) {
			handleAction(c, controllers.NewSchedulerAdjustAction())
		})
		router.POST(controllers.REQURI_TOPOLOGY_CHECK, func(c *gin.Context) {
			handleAction(c, controllers.NewTopologyAction())
		})
		router.POST(controllers.REQURI_TABLE_TOPOLOGY, func(c *gin.Context) {
			handleAction(c, controllers.NewTableTopologyAction())
		})
		router.POST(controllers.REQURI_TABLE_TOPOLOGY_CREATE, func(c *gin.Context) {
			handleAction(c, controllers.NewTopologyRangeCreateAction())
		})
		router.POST(controllers.RANGE_DUPLICATE_GET, func(c *gin.Context) {
			handleAction(c, controllers.NewRangeDuplicateAction())
		})
		router.POST(controllers.REQURI_CLUSTER_TOPOLOGY_GETALL, func(c *gin.Context) {
			handleAction(c, controllers.NewTopologyViewAction())
		})
		router.GET(controllers.REQURI_TASK_GETTASKTYPEALL, func(c *gin.Context) {
			handleAction(c, controllers.NewTaskTypeAllAction())
		})
		router.POST(controllers.REQURI_DB_CONSOLE_QUERY, func(c *gin.Context) {
			handleAction(c, controllers.NewStoreDataQuery())
		})

		// user
		router.GET(controllers.REQURI_USER_ADMIN, func(c *gin.Context) {
			handleAction(c, controllers.NewUserAdminAction())
		})
		router.POST(controllers.REQURI_USER_GETUSERLIST, func(c *gin.Context) {
			handleAction(c, controllers.NewStoreDataQuery())
		})
		router.GET(controllers.REQURI_USER_GETPRIVILEGELIST, func(c *gin.Context) {
			handleAction(c, controllers.NewPrivilegeInfoAction())
		})
		router.POST(controllers.REQURI_USER_UPDATEPRIVILEG, func(c *gin.Context) {
			handleAction(c, controllers.NewPrivilegeUpdateAction())
		})
		router.POST(controllers.REQURI_USER_DELRIVILEGS, func(c *gin.Context) {
			handleAction(c, controllers.NewPrivilegeDelAction())
		})
		router.GET(controllers.REQURI_USER_GETROLELIST, func(c *gin.Context) {
			handleAction(c, controllers.NewRoleInfoAction())
		})
		router.POST(controllers.REQURI_USER_ADDROLE, func(c *gin.Context) {
			handleAction(c, controllers.NewRoleAddAction())
		})
		router.POST(controllers.REQURI_USER_DELROLE, func(c *gin.Context) {
			handleAction(c, controllers.NewRoleDelAction())
		})
	}

	//sql apply
	router.GET(controllers.REQURI_SQL_GETALL, func(c *gin.Context) {
		handleAction(c, controllers.NewSqlGetAllAction())
	})
	router.POST(controllers.REQURI_SQL_APPLY, func(c *gin.Context) {
		handleAction(c, controllers.NewSqlApplyAction())
	})
	router.GET(controllers.REQURI_SQL_APPLY_DETAIL, func(c *gin.Context) {
		handleAction(c, controllers.NewSqlApplyGetAction())
	})
	router.POST(controllers.REQURI_SQL_APPLY_AUDIT, func(c *gin.Context) {
		handleAction(c, controllers.NewSqlAuditAction())
	})

	//lock
	router.GET(controllers.REQURI_LOCK_NAMESPACE_GETALL, func(c *gin.Context) {
		handleAction(c, controllers.NewLockGetAllNspAction())
	})
	router.POST(controllers.REQURI_LOCK_NAMESPACE_APPLY, func(c *gin.Context) {
		handleAction(c, controllers.NewLockNspApplyAction())
	})
	router.POST(controllers.REQURI_LOCK_NAMESPACE_AUDIT, func(c *gin.Context) {
		handleAction(c, controllers.NewLockNspAuditAction())
	})
	router.POST(controllers.REQURI_LOCK_NAMESPACE_UPDATE, func(c *gin.Context) {
		handleAction(c, controllers.NewLockNspUpdateAction())
	})
	router.POST(controllers.REQURI_LOCK_NAMESPACE_DELETE, func(c *gin.Context) {
		handleAction(c, controllers.NewLockNspDeleteAction())
	})
	router.GET(controllers.REQURI_LOCK_CLUSTER_LIST, func(c *gin.Context) {
		handleAction(c, controllers.NewLockClusterListGetAction())
	})
	router.POST(controllers.REQURI_LOCK_CLUSTER_INFO, func(c *gin.Context) {
		handleAction(c, controllers.NewLockClusterInfoGetAction())
	})
	router.GET(controllers.REQURI_LOCK_LOCK_GETALL, func(c *gin.Context) {
		handleAction(c, controllers.NewLockGetAllAction())
	})
	router.POST(controllers.REQURI_LOCK_LOCK_FORCEUNLOCK, func(c *gin.Context) {
		handleAction(c, controllers.NewLockForceUnLockAction())
	})
	router.POST(controllers.REQURI_LOCK_CLIENT_TOKEN, func(c *gin.Context) {
		handleAction(c, controllers.NewLockClientGetTokenAction())
	})

	//configure
	router.GET(controllers.REQURI_CONFIGURE_NAMESPACE_GETALL, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureGetAllNspAction())
	})
	router.POST(controllers.REQURI_CONFIGURE_NAMESPACE_APPLY, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureNspApplyAction())
	})
	router.POST(controllers.REQURI_CONFIGURE_NAMESPACE_AUDIT, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureNspAuditAction())
	})
	router.POST(controllers.REQURI_CONFIGURE_NAMESPACE_UPDATE, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureNspUpdateAction())
	})
	router.POST(controllers.REQURI_CONFIGURE_NAMESPACE_DELETE, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureNspDeleteAction())
	})
	router.GET(controllers.REQURI_CONFIGURE_CLUSTER_LIST, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureClusterListGetAction())
	})
	router.POST(controllers.REQURI_CONFIGURE_CLUSTER_INFO, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureClusterInfoGetAction())
	})
	router.GET(controllers.REQURI_CONFIGURE_GETALL, func(c *gin.Context) {
		handleAction(c, controllers.NewConfigureGetAllAction())
	})

	//metric
	router.GET(controllers.REQURL_METRIC_SERVER_GETALL, func(c *gin.Context) {
		handleAction(c, controllers.NewGetMetricServerAction())
	})
	router.POST(controllers.REQURL_METRIC_SERVER_ADD, func(c *gin.Context) {
		handleAction(c, controllers.NewAddMetricServerAction())
	})
	router.POST(controllers.REQURL_METRIC_SERVER_DEL, func(c *gin.Context) {
		handleAction(c, controllers.NewDelMetricServerAction())
	})
	router.POST(controllers.REQURL_METRIC_CONFIG_GET, func(c *gin.Context) {
		handleAction(c, controllers.NewGetMetricConfigAction())
	})
	router.POST(controllers.REQURL_METRIC_CONFIG_SET, func(c *gin.Context) {
		handleAction(c, controllers.NewSetMetricConfigAction())
	})
	router.Run(":" + fmt.Sprint(r.config.ReqListenPort))

	return router
}

func html404(c *gin.Context) {
	c.HTML(http.StatusNotFound, "404.html", nil)
}

func html403(c *gin.Context) {
	c.HTML(http.StatusNotFound, "403.html", nil)
}

func html500(c *gin.Context) {
	c.HTML(http.StatusNotFound, "500.html", nil)
}

func handleAction(c *gin.Context, act controllers.Action) {
	if data, err := act.Execute(c); err != nil {
		if fErr, ok := err.(*common.FbaseError); ok == true {
			c.JSON(http.StatusOK, &controllers.Response{
				Code: fErr.Code,
				Msg:  fErr.Error(),
				Data: "",
			})
		} else if mErr, ok := err.(*mysql.MySQLError); ok == true {
			c.JSON(http.StatusOK, &controllers.Response{
				Code: common.INTERNAL_ERROR.Code,
				Msg:  mErr.Message,
				Data: "",
			})
		} else {
			log.Warn("Cannot transfer to fbaseError instance from err:%#v", err)
			c.JSON(http.StatusOK, &controllers.Response{
				Code: common.INTERNAL_ERROR.Code,
				Msg:  common.INTERNAL_ERROR.Msg,
				Data: "",
			})
		}
	} else {
		c.JSON(http.StatusOK, &controllers.Response{
			Code: common.OK.Code,
			Msg:  common.OK.Msg,
			Data: data,
		})
	}
}

func handleRightAndAction(r *Router, c *gin.Context, act controllers.Action) {
	userName := sessions.Default(c).Get("user_name").(string)
	if len(userName) == 0 {
		c.Redirect(http.StatusMovedPermanently, "/logout")
	}

	userRight, err := r.GetUserCluster(userName)
	if err != nil || userRight == nil {
		c.JSON(http.StatusOK, &controllers.Response{
			Code: common.NO_RIGHT.Code,
			Msg:  common.NO_RIGHT.Msg,
		})
	} else {
		c.Set("userRight", userRight)
		handleAction(c, act)
	}
}
