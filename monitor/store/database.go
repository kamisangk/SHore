package store

import (
	"shore-master/monitor/config"
	"shore-master/monitor/models"
	"shore-master/monitor/security"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Open 初始化 GORM SQLite 连接。
func Open(cfg config.Config) (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(cfg.Database.SQLitePath), &gorm.Config{})
}

// MigrateAndSeed 迁移当前表结构并写入默认数据。
func MigrateAndSeed(db *gorm.DB, cfg config.Config) error {
	if err := db.AutoMigrate(
		&models.SystemUser{},
		&models.SystemConfig{},
		&models.Server{},
	); err != nil {
		return err
	}

	if err := seedAdmin(db, cfg); err != nil {
		return err
	}

	if err := seedConfig(db, cfg); err != nil {
		return err
	}

	return nil
}

func seedAdmin(db *gorm.DB, cfg config.Config) error {
	var count int64
	if err := db.Model(&models.SystemUser{}).Count(&count).Error; err != nil {
		return err
	}

	if count > 0 {
		return nil
	}

	hash, err := security.HashPassword("admin123")
	if err != nil {
		return err
	}

	return db.Create(&models.SystemUser{
		Username:     "admin",
		PasswordHash: hash,
	}).Error
}

func seedConfig(db *gorm.DB, cfg config.Config) error {
	for _, definition := range OrderedSystemConfigDefinitions() {
		var existing models.SystemConfig
		err := db.Where("config_key = ?", definition.Key).First(&existing).Error
		if err == nil {
			if existing.Description == definition.Description {
				continue
			}

			if saveErr := db.Model(&existing).Update("description", definition.Description).Error; saveErr != nil {
				return saveErr
			}
			continue
		}

		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}

		configValue := resolveSeedConfigValue(definition, cfg)
		if createErr := db.Create(&models.SystemConfig{
			ConfigKey:   definition.Key,
			ConfigValue: configValue,
			Description: definition.Description,
		}).Error; createErr != nil {
			return createErr
		}
	}

	return nil
}

func resolveSeedConfigValue(definition SystemConfigDefinition, cfg config.Config) string {
	return definition.DefaultValue
}
