package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	DB *gorm.DB
)

func InitDB(ctx context.Context) error {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // 慢SQL阈值
			LogLevel:                  logger.Info, // 日志等级
			IgnoreRecordNotFoundError: true,        // 忽略 ErrRecordNotFound error
			ParameterizedQueries:      false,       // SQL 日志中不包含参数
			Colorful:                  true,        // 启用颜色
		},
	)
	dsn := "remoter:rz2200!_S10@tcp(10.45.116.209:4406)/test_ck?charset=utf8&parseTime=True&loc=Local"

	conn, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})
	if err != nil {
		return err
	}
	pgDB, err := conn.DB()
	if err != nil {
		return err
	}
	pgDB.SetMaxOpenConns(100)
	pgDB.SetMaxIdleConns(10)

	DB = conn

	return nil
}

func main() {
	ctx := context.Background()
	err := InitDB(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	g := gin.New()
	g.Use(gin.Logger(), gin.Recovery())

	group1 := g.Group("/group")
	group1.POST("/add", AddGroup)
	group1.POST("/update", UpdateGroup)
	group1.POST("/delete", Delete)

	log.Fatalln(g.Run(":8080"))
}

type ResJSON struct {
	Code    int    `json:"code"`
	Message string `json:"msg"`
	Data    any    `json:"data"`
}

func newResJSON() *ResJSON {
	return &ResJSON{
		Code:    1000,
		Message: "success",
		Data:    nil,
	}
}

type AssetGroup struct {
	ID          int        `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	CreatedAt   *time.Time `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
}

func (a *AssetGroup) TableName() string {
	return "asset_groups"
}

type Closure struct {
	Ancestor   int `json:"ancestor"`
	Descendant int `json:"descendant"`
	Depth      int `json:"depth"`
}

func (c *Closure) TableName() string {
	return "closure_table"
}

type AddGroupReq struct {
	ParentID    int    `json:"parent_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func AddGroup(c *gin.Context) {
	var (
		req AddGroupReq
		res = newResJSON()
	)
	if err := c.BindJSON(&req); err != nil {
		res.Code = 1001
		res.Message = "fail"
		res.Data = err.Error()
		c.JSON(200, res)
		return
	}
	group := AssetGroup{
		Name:        req.Name,
		Description: req.Description,
	}
	if err := DB.Transaction(func(tx *gorm.DB) error {
		// create group
		if tx := tx.Create(&group); tx.Error != nil {
			return tx.Error
		}

		// insert group self closure
		closure := Closure{
			Ancestor:   group.ID,
			Descendant: group.ID,
		}
		if tx := tx.Create(&closure); tx.Error != nil {
			return tx.Error
		}
		// insert group parent closure
		if req.ParentID != 0 {
			if tx := tx.Exec(`INSERT INTO closure_table (ancestor, descendant, depth) SELECT ancestor, ?, depth + 1 
				FROM closure_table WHERE descendant = ?;`, group.ID, req.ParentID); tx.Error != nil {
				return tx.Error
			}
		}
		return nil
	}); err != nil {
		res.Code = 1001
		res.Message = "fail"
		res.Data = err.Error()
		c.JSON(200, res)
		return
	}
	c.JSON(200, res)
}

type UpdateGroupReq struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func UpdateGroup(c *gin.Context) {
	var (
		req UpdateGroupReq
		res = newResJSON()
	)
	if err := c.BindJSON(&req); err != nil {
		res.Code = 1001
		res.Message = "fail"
		res.Data = err.Error()
		c.JSON(200, res)
		return
	}
	if tx := DB.Table("asset_groups").Where("id = ?", req.ID).
		Updates(map[string]any{"name": req.Name, "description": req.Description}); tx.Error != nil {
		res.Code = 1001
		res.Message = "fail"
		res.Data = tx.Error.Error()
		c.JSON(200, res)
		return
	}
	c.JSON(200, res)

}

func Delete(c *gin.Context) {
	var (
		req UpdateGroupReq
		res = newResJSON()
	)
	if err := c.BindJSON(&req); err != nil {
		res.Code = 1001
		res.Message = "fail"
		res.Data = err.Error()
		c.JSON(200, res)
		return
	}

	if err := DB.Transaction(func(tx *gorm.DB) error {
		if tx := tx.Exec("DELETE FROM closure_table WHERE descendant = ?;", req.ID); tx.Error != nil {
			return tx.Error
		}
		if tx := tx.Exec("DELETE FROM asset_groups WHERE id = ?;", req.ID); tx.Error != nil {
			return tx.Error
		}
		return nil
	}); err != nil {
		res.Code = 1001
		res.Message = "fail"
		res.Data = err.Error()
		c.JSON(200, res)
		return
	}
	c.JSON(200, res)
}
