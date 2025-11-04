-- AGV 故障信息表
CREATE TABLE IF NOT EXISTS faults (
    id INTEGER PRIMARY KEY AUTOINCREMENT,                 -- 唯一ID，主键，自增
    reporter_name TEXT NOT NULL,                          -- 发现人员
    fault_time TIMESTAMP NOT NULL,                        -- 故障发生时间
    vehicle_id TEXT NOT NULL,                             -- 车辆编号
    category TEXT NOT NULL,                               -- 错误类别
    status TEXT NOT NULL DEFAULT '待修复',                  -- 解决状态 (待修复, 处理中, 已修复)
    description TEXT NOT NULL,                            -- 故障详细描述
    solution TEXT,                                        -- 解决办法 (初步)
    resolution_log TEXT,                                  -- 处理记录
    responsible_person TEXT NOT NULL,                     -- 责任人
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP        -- 记录创建时间
);
