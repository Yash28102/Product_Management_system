// main.go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/your-org/product-management/config"
    "github.com/your-org/product-management/api"
    "github.com/your-org/product-management/db"
    "github.com/your-org/product-management/queue"
    "github.com/your-org/product-management/cache"
    "github.com/your-org/product-management/logger"
)

func main() {
    // Initialize logger
    logger := logger.NewLogger()
    
    // Load configuration
    cfg, err := config.Load()
    if err != nil {
        logger.Fatal("Failed to load configuration", err)
    }
    
    // Initialize database connection
    database, err := db.NewPostgresDB(cfg.Database)
    if err != nil {
        logger.Fatal("Failed to connect to database", err)
    }
    defer database.Close()
    
    // Initialize Redis cache
    cache, err := cache.NewRedisCache(cfg.Redis)
    if err != nil {
        logger.Fatal("Failed to connect to Redis", err)
    }
    defer cache.Close()
    
    // Initialize message queue
    messageQueue, err := queue.NewRabbitMQ(cfg.RabbitMQ)
    if err != nil {
        logger.Fatal("Failed to connect to RabbitMQ", err)
    }
    defer messageQueue.Close()
    
    // Initialize API server
    server := api.NewServer(cfg.Server, database, cache, messageQueue, logger)
    
    // Start server in a goroutine
    go func() {
        if err := server.Start(); err != nil {
            logger.Fatal("Failed to start server", err)
        }
    }()
    
    // Initialize image processor
    processor := NewImageProcessor(cfg.ImageProcessor, database, messageQueue, logger)
    go processor.Start()
    
    // Graceful shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit
    
    ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
    defer cancel()
    
    if err := server.Shutdown(ctx); err != nil {
        logger.Error("Server forced to shutdown:", err)
    }
    
    logger.Info("Server exiting")
}

// config/config.go
package config

import (
    "github.com/spf13/viper"
)

type Config struct {
    Server         ServerConfig
    Database       DatabaseConfig
    Redis          RedisConfig
    RabbitMQ       RabbitMQConfig
    ImageProcessor ImageProcessorConfig
    ShutdownTimeout time.Duration
}

func Load() (*Config, error) {
    viper.SetConfigName("config")
    viper.SetConfigType("yaml")
    viper.AddConfigPath(".")
    
    if err := viper.ReadInConfig(); err != nil {
        return nil, err
    }
    
    var config Config
    if err := viper.Unmarshal(&config); err != nil {
        return nil, err
    }
    
    return &config, nil
}

// models/product.go
package models

type Product struct {
    ID                     uint      `json:"id" gorm:"primaryKey"`
    UserID                 uint      `json:"user_id"`
    ProductName            string    `json:"product_name"`
    ProductDescription     string    `json:"product_description"`
    ProductImages          []string  `json:"product_images" gorm:"type:jsonb"`
    CompressedProductImages []string  `json:"compressed_product_images" gorm:"type:jsonb"`
    ProductPrice           float64   `json:"product_price"`
    CreatedAt             time.Time `json:"created_at"`
    UpdatedAt             time.Time `json:"updated_at"`
}

// api/server.go
package api

import (
    "github.com/gin-gonic/gin"
    "github.com/your-org/product-management/middleware"
)

type Server struct {
    router  *gin.Engine
    db      *db.Database
    cache   *cache.Cache
    queue   *queue.MessageQueue
    logger  *logger.Logger
}

func NewServer(cfg ServerConfig, db *db.Database, cache *cache.Cache, queue *queue.MessageQueue, logger *logger.Logger) *Server {
    server := &Server{
        router: gin.New(),
        db:     db,
        cache:  cache,
        queue:  queue,
        logger: logger,
    }
    
    server.setupRoutes()
    return server
}

func (s *Server) setupRoutes() {
    // Middleware
    s.router.Use(middleware.RequestLogger(s.logger))
    s.router.Use(middleware.Recovery(s.logger))
    
    // Routes
    api := s.router.Group("/api/v1")
    {
        api.POST("/products", s.createProduct)
        api.GET("/products/:id", s.getProduct)
        api.GET("/products", s.listProducts)
    }
}

// api/handlers.go
func (s *Server) createProduct(c *gin.Context) {
    var product models.Product
    if err := c.ShouldBindJSON(&product); err != nil {
        s.handleError(c, err)
        return
    }
    
    // Start database transaction
    tx := s.db.Begin()
    
    if err := tx.Create(&product).Error; err != nil {
        tx.Rollback()
        s.handleError(c, err)
        return
    }
    
    // Publish image processing message
    if err := s.queue.PublishImageProcessing(product.ID, product.ProductImages); err != nil {
        tx.Rollback()
        s.handleError(c, err)
        return
    }
    
    tx.Commit()
    
    c.JSON(http.StatusCreated, product)
}

func (s *Server) getProduct(c *gin.Context) {
    id := c.Param("id")
    
    // Try to get from cache first
    if product, err := s.cache.GetProduct(id); err == nil {
        c.JSON(http.StatusOK, product)
        return
    }
    
    // If not in cache, get from database
    var product models.Product
    if err := s.db.First(&product, id).Error; err != nil {
        s.handleError(c, err)
        return
    }
    
    // Store in cache for future requests
    if err := s.cache.SetProduct(id, product); err != nil {
        s.logger.Error("Failed to cache product", err)
    }
    
    c.JSON(http.StatusOK, product)
}

// processor/image_processor.go
package processor

type ImageProcessor struct {
    db      *db.Database
    queue   *queue.MessageQueue
    logger  *logger.Logger
    config  *config.ImageProcessorConfig
}

func (p *ImageProcessor) Start() {
    messages := p.queue.ConsumeImageProcessing()
    
    for msg := range messages {
        go p.processImage(msg)
    }
}

func (p *ImageProcessor) processImage(msg queue.Message) {
    defer p.handlePanic()
    
    // Download image
    imgData, err := p.downloadImage(msg.ImageURL)
    if err != nil {
        p.logger.Error("Failed to download image", err)
        p.queue.Nack(msg)
        return
    }
    
    // Compress image
    compressed, err := p.compressImage(imgData)
    if err != nil {
        p.logger.Error("Failed to compress image", err)
        p.queue.Nack(msg)
        return
    }
    
    // Upload to S3
    url, err := p.uploadToS3(compressed)
    if err != nil {
        p.logger.Error("Failed to upload image", err)
        p.queue.Nack(msg)
        return
    }
    
    // Update database
    if err := p.db.UpdateCompressedImage(msg.ProductID, url); err != nil {
        p.logger.Error("Failed to update database", err)
        p.queue.Nack(msg)
        return
    }
    
    // Invalidate cache
    if err := p.cache.InvalidateProduct(msg.ProductID); err != nil {
        p.logger.Error("Failed to invalidate cache", err)
    }
    
    p.queue.Ack(msg)
}

// cache/redis.go
package cache

import (
    "context"
    "encoding/json"
    "time"
    
    "github.com/go-redis/redis/v8"
)

type Cache struct {
    client  *redis.Client
    ctx     context.Context
}

func (c *Cache) GetProduct(id string) (*models.Product, error) {
    data, err := c.client.Get(c.ctx, productKey(id)).Bytes()
    if err != nil {
        return nil, err
    }
    
    var product models.Product
    if err := json.Unmarshal(data, &product); err != nil {
        return nil, err
    }
    
    return &product, nil
}

func (c *Cache) SetProduct(id string, product models.Product) error {
    data, err := json.Marshal(product)
    if err != nil {
        return err
    }
    
    return c.client.Set(c.ctx, productKey(id), data, time.Hour).Err()
}

func (c *Cache) InvalidateProduct(id string) error {
    return c.client.Del(c.ctx, productKey(id)).Err()
}

// logger/logger.go
package logger

import (
    "go.uber.org/zap"
)

type Logger struct {
    *zap.SugaredLogger
}

func NewLogger() *Logger {
    config := zap.NewProductionConfig()
    config.OutputPaths = []string{"stdout", "logs/app.log"}
    
    logger, err := config.Build()
    if err != nil {
        panic(err)
    }
    
    return &Logger{logger.Sugar()}
}
