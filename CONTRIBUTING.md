# 贡献指南

感谢你对 MDB 项目的关注！我们欢迎各种形式的贡献。

## 如何贡献

### 报告问题

1. 在 [Issues](https://gitee.com/mdb-official/mdb/issues) 中搜索是否已有相同问题
2. 如果没有，请创建新 Issue，包含：
   - 问题描述
   - 复现步骤
   - 期望行为 vs 实际行为
   - 环境信息（OS、Rust 版本、MDB 版本）
   - 相关日志或截图

### 提交代码

1. Fork 本仓库
2. 创建特性分支：`git checkout -b feature/my-feature`
3. 提交更改：`git commit -m 'feat: add my feature'`
4. 推送到分支：`git push origin feature/my-feature`
5. 创建 Pull Request

### 提交信息规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 格式：

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

类型：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式（不影响功能）
- `refactor`: 重构（不新增功能/修复 bug）
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建/工具链更新

示例：
```
feat(storage): add LZ4 block compression
fix(query): correct $regex Unicode matching
docs(readme): add quick start guide
perf(bson): SIMD accelerate document comparison
```

### 代码规范

- **禁止 unsafe**：所有 crate 必须 `#![forbid(unsafe_code)]`
- **零外部依赖**：新增第三方依赖需 RFC 审核
- **测试覆盖**：新功能必须附带测试
- **文档注释**：公开 API 必须有 `///` 文档注释
- **代码风格**：遵循 `rustfmt` 默认格式

### 开发环境

```bash
# 克隆仓库
git clone https://gitee.com/mdb-official/mdb.git
cd mdb

# 编译
cargo build

# 运行测试
cargo test

# 代码检查
cargo clippy -- -D warnings

# 格式化
cargo fmt
```

### 分支策略

- `main`: 稳定版本，随时可发布
- `develop`: 开发分支，新功能合入此分支
- `feature/*`: 特性分支
- `fix/*`: 修复分支
- `release/*`: 发布准备分支

### 发布流程

1. 从 `develop` 创建 `release/vX.Y.Z` 分支
2. 更新版本号和 CHANGELOG
3. 测试通过后合入 `main` 并打 tag
4. 从 `main` 合回 `develop`

## 行为准则

请遵守 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。

## 联系方式

- Issues: https://gitee.com/mdb-official/mdb/issues
- 邮箱: dev@mdb-project.com

## 许可证

贡献的代码将采用 Apache-2.0 许可证。
