### sql

```sql
-- 基础分组表
CREATE TABLE asset_groups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 分组关系表
CREATE TABLE closure_table (
    ancestor INT,
    descendant INT,
    depth INT,
    PRIMARY KEY (ancestor, descendant),
    FOREIGN KEY (ancestor) REFERENCES asset_groups(id),
    FOREIGN KEY (descendant) REFERENCES asset_groups(id)
);

-- 资产表
CREATE TABLE `assets` (
  id INT AUTO_INCREMENT PRIMARY KEY,
  asset_name varchar(100) NOT NULL,
  asset_type varchar(100) DEFAULT NULL,
  group_id INT DEFAULT NULL,
  FOREIGN KEY (group_id) REFERENCES asset_groups(id)
);

```

### 概念
在MySQL中，闭包表是一种处理树形结构的方式，能够高效支持任意深度的分组操作。闭包表法通过引入一张额外的闭包表来存储节点之间的祖先关系，使得可以快速地进行各种层级操作。闭包表通常包含以下几列：

- `ancestor`：表示某节点的祖先节点。
- `descendant`：表示某节点的后代节点。
- `depth`：表示祖先和后代之间的层级深度（`ancestor`等于`descendant`时，`depth`为0）。

### 闭包表结构设计

我们假设有一个资产管理树形结构，资产存储在`asset`表中，每个资产都有唯一的`id`。为管理资产的分组关系，我们引入一张`closure`表来存储资产的祖先后代关系。

#### 1. `asset`表结构
```sql
CREATE TABLE asset (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);
```

#### 2. `closure`表结构
```sql
CREATE TABLE closure (
    ancestor INT NOT NULL,
    descendant INT NOT NULL,
    depth INT NOT NULL,
    PRIMARY KEY (ancestor, descendant),
    FOREIGN KEY (ancestor) REFERENCES asset(id),
    FOREIGN KEY (descendant) REFERENCES asset(id)
);
```

- `ancestor`和`descendant`是外键，指向`asset`表的`id`。
- `depth`表示祖先与后代之间的层级深度，`depth=0`表示自己是自己的祖先。

### 操作SQL编写

#### 1. 插入新资产及维护闭包表

假设我们要插入一个新资产`B`，并且其父节点是`A`，我们需要将新节点插入`asset`表中，并在`closure`表中维护`A`和`B`的关系。

```sql
-- 插入新资产
INSERT INTO asset (name) VALUES ('B');

-- 获取新插入资产的ID
SET @newAssetId = LAST_INSERT_ID();

-- 假设父节点A的ID为1
SET @parentId = 1;

-- 插入闭包关系
-- 1. 将新节点的闭包表记录为自己与自己的关系（depth=0）
INSERT INTO closure (ancestor, descendant, depth)
VALUES (@newAssetId, @newAssetId, 0);

-- 2. 将新节点与其所有祖先的关系加入闭包表
INSERT INTO closure (ancestor, descendant, depth)
SELECT ancestor, @newAssetId, depth + 1
FROM closure
WHERE descendant = @parentId;
```

#### 2. 删除资产及维护闭包表

当删除某个资产时，闭包表中涉及该资产的所有祖先后代关系也要删除。

```sql
-- 假设要删除的资产ID为3

-- 1. 删除闭包表中该资产作为后代的所有关系
DELETE FROM closure WHERE descendant = 3;

-- 2. 删除资产表中的记录
DELETE FROM asset WHERE id = 3;
```

#### 3. 查询某个节点的所有子节点

要查询某个资产的所有子节点（包括所有层级的子节点），可以直接在`closure`表中查找。

```sql
-- 假设要查询ID为1的资产的所有子节点
SELECT a.id, a.name
FROM closure c
JOIN asset a ON c.descendant = a.id
WHERE c.ancestor = 1 AND c.depth > 0;
```

#### 4. 查询某个节点的所有父节点

类似地，要查询某个资产的所有父节点，也可以通过`closure`表查询。

```sql
-- 查询ID为3的资产的所有父节点
SELECT a.id, a.name
FROM closure c
JOIN asset a ON c.ancestor = a.id
WHERE c.descendant = 3 AND c.depth > 0;
```

#### 总结
闭包表通过额外存储祖先和后代节点的关系，能够高效支持树形结构的增删改查操作。插入时维护闭包关系，删除时清理相应的祖先-后代关系，而查询时可以通过简单的SQL快速得到所有层级的父节点或子节点。