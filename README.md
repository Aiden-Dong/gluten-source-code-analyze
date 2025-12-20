
#### 结构图

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Apache Gluten VeloxBackend 物理计划变更与执行流程                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              SPARK DRIVER                                                      │
├────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                    1. 插件初始化阶段                                                         │ │
│  └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                                 │
│    SparkSession.builder()                                                                                       │
│           ↓                                                                                                     │
│    GlutenPlugin.driverPlugin()                                                                                  │
│           ↓                                                                                                     │
│    GlutenDriverPlugin.init()                                                                                    │
│           ↓                                                                                                     │
│    GlutenSessionExtensions.apply()                                                                              │
│           ↓                                                                                                     │
│    Component.sorted().foreach(_.injectRules(injector))                                                          │
│           ↓                                                                                                     │
│    VeloxBackend.injectRules() → 注入Velox特定的优化规则                                                            │
│                                                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                    2. 查询计划优化阶段                                                        │ │
│  └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                                 │
│    SQL Query / DataFrame API                                                                                    │
│           ↓                                                                                                     │
│    Catalyst Parser → LogicalPlan                                                                                │
│           ↓                                                                                                     │
│    Catalyst Optimizer (with Gluten Rules)                                                                       │
│           ↓                                                                                                     │
│    SparkPlanner → Physical Plan                                                                                 │
│           ↓                                                                                                     │
│    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│    │                              Gluten 规则注入点                                                           │   │
│    │                                                                                                        │   │
│    │  ColumnarRuleApplier.apply()                                                                           │   │
│    │         ↓                                                                                              │   │
│    │  VeloxRuleApi.injectColumnarRules()                                                                    │   │
│    │         ↓                                                                                              │   │
│    │  - ColumnarToColumnarTransition                                                                        │   │
│    │  - FallbackPolicies                                                                                    │   │
│    │  - VeloxSpecificRules                                                                                  │   │
│    │         ↓                                                                                              │   │
│    │  Physical Plan Transformation:                                                                         │   │
│    │    ProjectExec → ProjectExecTransformer                                                                │   │
│    │    FilterExec → FilterExecTransformer                                                                  │   │
│    │    HashJoinExec → HashJoinExecTransformer                                                              │   │
│    │    ScanExec → FileSourceScanExecTransformer                                                            │   │
│    │    AggregateExec → HashAggregateExecTransformer                                                        │   │
│    └────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│           ↓                                                                                                     │
│    Optimized Physical Plan (with Gluten Transformers)                                                           │
│           ↓                                                                                                     │
│    Plan Validation:                                                                                             │
│      VeloxValidatorApi.doValidate()                                                                             │
│           ↓                                                                                                     │
│    Task Generation & Distribution                                                                               │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

                                                    ↓ RDD.compute()

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             SPARK EXECUTOR                                                      │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                    3. Executor 初始化                                                       │ │
│  └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                                 │
│    GlutenExecutorPlugin.init()                                                                                  │
│           ↓                                                                                                     │
│    Component.sorted().foreach(_.onExecutorStart())                                                              │
│           ↓                                                                                                     │
│    VeloxBackend.onExecutorStart()                                                                               │
│           ↓                                                                                                     │
│    VeloxRuntime.initialize() → 初始化Velox原生运行时                                                               │
│           ↓                                                                                                     │
│    Memory Pool & Configuration Setup                                                                            │
│                                                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                    4. 任务执行阶段                                                           │ │
│  └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                                 │
│    Task.run()                                                                                                   │
│           ↓                                                                                                     │
│    GlutenPlan.doExecuteColumnar()                                                                               │
│           ↓                                                                                                     │
│    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│    │                              Substrait 计划生成                                                         │   │
│    │                                                                                                        │   │
│    │  TransformerApi.createSubstraitPlan()                                                                  │   │
│    │         ↓                                                                                              │   │
│    │  VeloxTransformerApi.createSubstraitPlan()                                                             │   │
│    │         ↓                                                                                              │   │
│    │  SubstraitContext.build()                                                                              │   │
│    │         ↓                                                                                              │   │
│    │  Physical Plan → Substrait Plan 转换:                                                                   │   │
│    │    - ProjectExecTransformer → ProjectRel                                                               │   │
│    │    - FilterExecTransformer → FilterRel                                                                 │   │
│    │    - HashJoinExecTransformer → JoinRel                                                                 │   │
│    │    - FileSourceScanExecTransformer → ReadRel                                                           │   │
│    │    - HashAggregateExecTransformer → AggregateRel                                                       │   │
│    │         ↓                                                                                              │   │
│    │  Substrait Plan (Protobuf)                                                                             │   │
│    └────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│           ↓                                                                                                     │
│    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│    │                              JNI 调用层                                                                 │   │
│    │                                                                                                        │   │
│    │  VeloxIteratorApi.genNativeIterator()                                                                  │   │
│    │         ↓                                                                                              │   │
│    │  JniWrapper.nativeCreateKernelWithIterator()                                                           │   │
│    │         ↓                                                                                              │   │
│    │  JNI Call: Java → C++                                                                                  │   │
│    │    - Substrait Plan (byte[])                                                                           │   │
│    │    - Configuration (Map<String, String>)                                                               │   │
│    │    - Input Iterators                                                                                   │   │
│    └────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│           ↓                                                                                                     │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

                                                    ↓ JNI Bridge

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           VELOX NATIVE RUNTIME                                                  │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                    5. 原生执行引擎                                                           │ │
│  └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                                 │
│    cpp/velox/jni/VeloxJniWrapper.cc                                                                             │
│           ↓                                                                                                     │
│    JniWrapper::nativeCreateKernelWithIterator()                                                                 │
│           ↓                                                                                                     │
│    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│    │                              Substrait 解析                                                             │   │
│    │                                                                                                        │   │
│    │  cpp/velox/substrait/SubstraitToVeloxPlan.cc                                                           │   │
│    │         ↓                                                                                              │   │
│    │  SubstraitParser::parseSubstraitPlan()                                                                 │   │
│    │         ↓                                                                                              │   │
│    │  Substrait Plan → Velox Plan 转换:                                                                      │   │
│    │    - ReadRel → TableScanNode                                                                           │   │
│    │    - ProjectRel → ProjectNode                                                                          │   │
│    │    - FilterRel → FilterNode                                                                            │   │
│    │    - JoinRel → HashJoinNode                                                                            │   │
│    │    - AggregateRel → AggregationNode                                                                    │   │
│    │         ↓                                                                                              │   │
│    │  Velox Execution Plan Tree                                                                             │   │
│    └────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│           ↓                                                                                                     │
│    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│    │                              Velox 执行引擎                                                              │   │
│    │                                                                                                        │   │
│    │  velox/exec/Task.cpp                                                                                   │   │
│    │         ↓                                                                                              │   │
│    │  Task::start() → 启动执行任务                                                                            │   │
│    │         ↓                                                                                              │   │
│    │  Driver::runInternal() → 驱动执行                                                                        │   │
│    │         ↓                                                                                              │   │
│    │  Operator Pipeline 执行:                                                                                │   │
│    │    ┌───────────────────────────────────────────────────────────────────────────────────────────────┐   │   │
│    │    │  TableScan → Project → Filter → HashJoin → Aggregate → Output                                 │   │   │
│    │    │      ↓           ↓        ↓         ↓           ↓          ↓                                  │   │   │
│    │    │  读取数据    列投影    行过滤    哈希连接    聚合计算    结果输出                                     │   │   │
│    │    │      ↓           ↓        ↓         ↓           ↓          ↓                                  │   │   │
│    │    │  Parquet/    Vector     Vector     Hash       Vector     Arrow                                │   │   │
│    │    │  ORC读取     处理       处理       Table      处理       Batch                                   │   │   │
│    │    └───────────────────────────────────────────────────────────────────────────────────────────────┘   │   │
│    │         ↓                                                                                              │   │
│    │  Memory Management:                                                                                    │   │
│    │    - VeloxMemoryManager                                                                                │   │
│    │    - Arrow Memory Pool                                                                                 │   │
│    │    - Off-heap Memory Allocation                                                                        │   │
│    └────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│           ↓                                                                                                     │
│    ColumnarBatch (Arrow Format)                                                                                 │
│           ↓                                                                                                     │
│    Return to JVM via JNI                                                                                        │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

                                                    ↓ JNI Return

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                        SPARK EXECUTOR (返回阶段)                                                  │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                    6. 结果处理与返回                                                          │ │
│  └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                                                 │
│    Native Iterator Returns                                                                                      │
│           ↓                                                                                                     │
│    VeloxColumnarBatch (Arrow Format)                                                                            │
│           ↓                                                                                                     │
│    ColumnarToColumnarExec.doExecuteColumnar()                                                                   │
│           ↓                                                                                                     │
│    ┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│    │                              数据格式转换 (如需要)                                                        │   │
│    │                                                                                                        │   │
│    │  VeloxBatchType → SparkBatchType                                                                       │   │
│    │         ↓                                                                                              │   │
│    │  ColumnarToColumnarTransition.apply()                                                                  │   │
│    │         ↓                                                                                              │   │
│    │  Arrow Batch → Spark ColumnarBatch                                                                     │   │
│    │  (如果需要与非Gluten算子交互)                                                                              │   │
│    └────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│           ↓                                                                                                     │
│    Iterator[ColumnarBatch]                                                                                      │
│           ↓                                                                                                     │
│    Task Result                                                                                                  │
│           ↓                                                                                                     │
│    Return to Driver                                                                                             │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                            关键类与模块映射                                                        │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  Driver端核心类:                                                                                                 │
│    • GlutenPlugin                    → gluten-core/src/main/scala/org/apache/gluten/                            │
│    • GlutenSessionExtensions         → gluten-core/src/main/scala/org/apache/gluten/extension/                  │
│    • VeloxBackend                    → backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/       │
│    • ColumnarRuleApplier             → gluten-core/src/main/scala/org/apache/gluten/extension/columnar/         │
│    • VeloxValidatorApi               → backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/       │
│                                                                                                                 │
│  Executor端核心类:                                                                                                │
│    • GlutenExecutorPlugin            → gluten-core/src/main/scala/org/apache/gluten/                            │
│    • VeloxSparkPlanExecApi           → backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/       │
│    • VeloxIteratorApi                → backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/       │
│    • VeloxTransformerApi             → backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/       │
│    • ColumnarToColumnarExec          → gluten-core/src/main/scala/org/apache/gluten/execution/                  │
│                                                                                                                 │
│  JNI层核心文件:                                                                                                   │
│    • JniWrapper.cc                   → cpp/core/jni/                                                            │
│    • VeloxJniWrapper.cc              → cpp/velox/jni/                                                           │
│                                                                                                                 │
│  Velox原生层核心文件:                                                                                              │
│    • SubstraitToVeloxPlan.cc         → cpp/velox/substrait/                                                     │
│    • VeloxPlanConverter.cc           → cpp/velox/compute/                                                       │
│    • VeloxRuntime.cc                 → cpp/velox/compute/                                                       │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                            执行流程时序图                                                          │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  Driver                    Executor                    JNI                     Velox                            │
│    │                         │                         │                        │                               │
│    │ 1. Plugin Init          │                         │                        │                               │
│    │────────────────────────→│                         │                        │                               │
│    │                         │ 2. Component Init       │                        │                               │
│    │                         │────────────────────────→│                        │                               │
│    │                         │                         │ 3. Runtime Init        │                               │
│    │                         │                         │───────────────────────→│                               │
│    │ 4. Plan Optimization    │                         │                        │                               │
│    │ (Rules Injection)       │                         │                        │                               │
│    │                         │                         │                        │                               │
│    │ 5. Task Distribution    │                         │                        │                               │
│    │────────────────────────→│                         │                        │                               │
│    │                         │ 6. Execute Columnar     │                        │                               │
│    │                         │ (Substrait Generation)  │                        │                               │
│    │                         │                         │                        │                               │
│    │                         │ 7. JNI Call             │                        │                               │
│    │                         │────────────────────────→│                        │                               │
│    │                         │                         │ 8. Plan Conversion     │                               │
│    │                         │                         │───────────────────────→│                               │
│    │                         │                         │                        │ 9. Native Execution           │
│    │                         │                         │                        │ (Velox Operators)             │
│    │                         │                         │                        │                               │
│    │                         │                         │ 10. Results Return     │                               │
│    │                         │                         │←───────────────────────│                               │
│    │                         │ 11. ColumnarBatch       │                        │                               │
│    │                         │←────────────────────────│                        │                               │
│    │ 12. Task Results        │                         │                        │                               │
│    │←────────────────────────│                         │                        │                               │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```