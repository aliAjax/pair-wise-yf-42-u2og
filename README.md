# 动物园谱系与繁育协调

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8308`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8308
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `animal`：个体谱系；`pairing`：配对建议；`transfer`：机构和运输记录。

## 两园对账同步（合笼前对档）

两园各持谱系时，繁育协调员通过同步批次一次性导入多只动物和配对，
协调器在逐项事务中做去重合并、配对去占和批准失效处理。

- `POST /api/sync`：提交批次，请求体为
  `{"sync_key":"批次号","source_system":"Zoo-A","animals":[...],"pairings":[...]}`。
  同一 `sync_key` 重放是幂等的：失败后续跑从检查点继续，重复重试不会
  多建动物或重复占用配对槽位。
- 动物去重：批次内同名、同 `studbook_id`、`alias_refs`，以及与在档动物
  同名/同谱系号的记录会合并为一只规范个体；外来本地编号会建成 `merged`
  占位并写入重定向。所有旧编号去向可通过 `GET /api/source-refs`
  （可加 `/<canonical_id>` 过滤）查询。
- 配对去重：`(sire, dam)` 槽位全库唯一，两园并发提交同一配对只有一笔
  生效。可用 `GET /api/pairing-slots/<sire>/<dam>` 查询占用方。
- 批准失效：动物血缘（父母）更新后，引用该血缘且已 `approved` 的配对退回
  新状态 `needs_confirmation`，原批准进入 `approval_history`，需要重新
  执行 `approve`；同步过来的已批准配对若父母变了同样失效。
- 合并后个体的旧编号在读取时（`GET /api/entities/<id>`）自动重定向到规范
  个体；配对、运输中的引用及配对槽位一并改写到合并后的个体。
- `GET /api/sync/<sync_key>`：查看批次和每个条目的检查点状态。
- 只有 `admin`、`coordinator`、`registrar` 角色可以提交同步批次。

### 同步记录字段

- 动物：`source_id`（必填，来源园编号）、`id`（可选本地编号）、`name`、
  `sex`、`studbook_id`、`sire_ref`/`dam_ref`（可引用本批或来源编号，
  跨系统编号唯一时自动识别）、`alias_refs`、`birth_date`。
- 配对：`source_id`（可省，缺省由父母编号派生）、`sire_ref`、`dam_ref`、
  `status`（`approved` 或缺省的 `proposed`）、`approvals`、`proposed_by`。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

谱系系数是简化亲缘规则，不替代专业谱系软件、遗传咨询或法定动物运输许可。
