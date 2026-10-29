package admin

import (
	"github.com/gin-gonic/gin"
	"lxcapi/internal/db"
	"lxcapi/models"
	"lxcapi/pkg/response"
)

// GetTaskList 获取任务列表
func GetTaskList(c *gin.Context) {
	var tasks []models.Task
	
	query := db.DB.Order("id DESC")
	
	containerName := c.Query("container_name")
	if containerName != "" {
		query = query.Where("container_name = ?", containerName)
	}
	
	if err := query.Find(&tasks).Error; err != nil {
		response.Error(c, 500, "获取任务列表失败")
		return
	}

	response.Success(c, gin.H{
		"tasks": tasks,
		"total": len(tasks),
	})
}

// GetTask 获取任务详情
func GetTask(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		response.Error(c, 400, "缺少任务ID")
		return
	}

	var task models.Task
	if err := db.DB.Where("id = ?", taskID).First(&task).Error; err != nil {
		response.Error(c, 404, "任务不存在")
		return
	}

	response.Success(c, task)
}

// DeleteTask 删除任务（仅允许删除已结束的任务）
func DeleteTask(c *gin.Context) {
	taskID := c.Param("id")

	if taskID == "" {
		response.Error(c, 400, "缺少任务ID")
		return
	}

	var task models.Task
	if err := db.DB.Select("id", "status").Where("id = ?", taskID).First(&task).Error; err != nil {
		response.Error(c, 404, "任务不存在")
		return
	}

	if task.Status == models.TaskQueued || task.Status == models.TaskRunning {
		response.Error(c, 400, "任务正在执行或排队中，无法删除")
		return
	}

	if err := db.DB.Delete(&models.Task{}, "id = ?", taskID).Error; err != nil {
		response.Error(c, 500, "删除任务失败")
		return
	}
	response.Success(c, "任务删除成功")
}

// BatchDeleteTasks 批量删除任务
func BatchDeleteTasks(c *gin.Context) {
	var req struct {
		TaskIDs []string `json:"task_ids"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, 400, "参数错误")
		return
	}
	
	if len(req.TaskIDs) == 0 {
		response.Error(c, 400, "请选择要删除的任务")
		return
	}

	var activeCount int64
	if err := db.DB.Model(&models.Task{}).
		Where("id IN ? AND status IN ?", req.TaskIDs, []string{models.TaskQueued, models.TaskRunning}).
		Count(&activeCount).Error; err != nil {
		response.Error(c, 500, "校验任务状态失败")
		return
	}
	if activeCount > 0 {
		response.Error(c, 400, "包含正在执行或排队中的任务，无法删除")
		return
	}

	result := db.DB.Delete(&models.Task{}, "id IN ?", req.TaskIDs)
	if result.Error != nil {
		response.Error(c, 500, "批量删除失败")
		return
	}

	response.Success(c, gin.H{
		"deleted": result.RowsAffected,
		"message": "批量删除成功",
	})
}
