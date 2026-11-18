"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, window_metric

# 固定路径的窗口指标路由必须先于 /api/{module} 资源路由注册。
ROUTERS = [window_metric, resource]
