# easyswoole/pool

基于 Swoole 协程 Channel 的通用对象池，适用于数据库、Redis、HTTP 客户端等资源复用。池负责创建、借出、归还、健康检查和低负载缩容；具体连接的建立、探活、状态恢复和关闭由业务对象实现。

- [安装与运行环境](#安装与运行环境)
- [快速开始](#快速开始)
- [连接池配置](#连接池配置)
- [对象生命周期](#对象生命周期)
- [借用与归还](#借用与归还)
- [预热与健康检查](#预热与健康检查)
- [自动缩容与状态](#自动缩容与状态)
- [池管理器](#池管理器)
- [销毁与重置](#销毁与重置)
- [特别注意事项](#特别注意事项)
- [运行测试](#运行测试)

## 安装与运行环境

```bash
composer require easyswoole/pool
```

当前 `composer.json` 要求 PHP >= 8.1、JSON 扩展，以及 `easyswoole/spl` ^2.1、`easyswoole/utility` ^1.4、`easyswoole/component` ^2.4。

实际运行还需要 Swoole 扩展及其 Coroutine、Channel、Timer 功能；当前 Composer 依赖没有声明 `ext-swoole`，安装成功不代表运行环境已就绪。对象借用、归还、预热及依赖协程上下文的操作应在 Swoole 协程内执行。实际客户端如需协程 hook，应按该客户端要求启用。

## 快速开始

下面是无需数据库或 Redis 的完整示例。业务对象必须实现 `ObjectInterface` 的四个方法，`createObject()` 必须返回该接口的实例。

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use EasySwoole\Pool\AbstractPool;
use EasySwoole\Pool\Config;
use EasySwoole\Pool\ObjectInterface;

class DemoObject implements ObjectInterface
{
    private bool $open = true;
    public array $context = [];

    public function beforeUse(): bool
    {
        return $this->open;
    }

    public function intervalCheck(): bool
    {
        return $this->open;
    }

    public function objectRestore(): void
    {
        $this->context = [];
    }

    public function gc(): void
    {
        $this->open = false;
    }

    public function execute(): string
    {
        if (!$this->open) {
            throw new RuntimeException('对象已关闭');
        }
        return 'ok';
    }
}

class DemoPool extends AbstractPool
{
    protected function createObject(): ObjectInterface
    {
        // 实际连接在这里建立；失败时抛出异常。
        return new DemoObject();
    }
}

$config = new Config([
    'minObjectNum' => 2,
    'maxObjectNum' => 16,
    'getObjectTimeout' => 3.0,
    'intervalCheckTime' => 5000,
    'intervalCheckBatchSize' => 8,
]);
$pool = new DemoPool($config);

Swoole\Coroutine\run(function () use ($pool) {
    try {
        $pool->keepMin();
        $result = $pool->invoke(function (DemoObject $object) {
            $object->context['request'] = 'demo';
            return $object->execute();
        });
        echo $result . PHP_EOL;
    } catch (Throwable $error) {
        error_log($error->getMessage());
    } finally {
        // 独立脚本结束时停止定时器并清理空闲对象。
        // 常驻服务在停机时处理，不要每个请求都 destroy。
        $pool->destroy();
    }
});
```

接入 Redis 等客户端时，使用适配器实现 `ObjectInterface`，在适配器中持有客户端：`beforeUse()` / `intervalCheck()` 探活，`objectRestore()` 恢复业务状态，`gc()` 关闭连接。客户端自身没有实现该接口时，不能直接从 `createObject()` 返回它。当前项目没有 `MagicPool`。

## 连接池配置

配置通过构造数组或对应 setter 设置；setter 返回 `Config`，可链式调用。

| 配置项 | 默认值 | 单位与作用 |
| --- | --- | --- |
| `maxObjectNum` | `16` | 最大对象数，包含已借出、空闲及正在创建的名额 |
| `minObjectNum` | `8` | 预热及周期补充的目标总数量，不是空闲对象数量 |
| `getObjectTimeout` | `3.0` | 秒，默认 Channel 获取等待时间 |
| `intervalCheckTime` | `5000` | 毫秒，健康检查及补充最小数量的定时周期；`<= 0` 关闭此定时器 |
| `intervalCheckBatchSize` | `8` | 每轮最多检查的空闲对象数，必须大于 0 |
| `waitLoadAverageTime` | `0.001` | 秒，平均 Channel 等待时间低于此值时尝试缩容 |
| `extraConf` | `null` | 自定义配置，池不自动解释它 |

```php
$config = new Config([
    'minObjectNum' => 0,
    'maxObjectNum' => 4,
]);
$config->setIntervalCheckBatchSize(2)
    ->setIntervalCheckTime(1000)
    ->setGetObjectTimeout(0.5)
    ->setExtraConf(['endpoint' => 'service.example']);
```

`minObjectNum` 必须严格小于 `maxObjectNum`，两者相等也会被拒绝。使用 setter 降低最大数量时，应先降低最小数量；更建议用构造数组一起设置。业务应保证 `0 <= minObjectNum < maxObjectNum`；当前代码没有对所有负数或非法时间值做完整校验。

旧版本示例中的 `maxIdleTime`、`loadAverageTime` 不是当前配置项，当前也没有 `idleCheck()`。这里的缩容是根据平均等待时间判断，不是按对象闲置时长淘汰。

## 对象生命周期

| 实现点 | 何时调用 | 业务职责与失败行为 |
| --- | --- | --- |
| `createObject(): ObjectInterface` | 无空闲对象且容量允许，或预热补充 | 建立可用资源；创建失败释放预占名额并抛出异常 |
| `beforeUse(): bool` | Channel 出队后、交给调用方前 | 检查资源；`false` 或异常会清理对象并按获取逻辑重试 |
| `objectRestore()` | 归还对象时 | 清理请求状态、结束未完成事务等；异常时尝试报告并丢弃对象 |
| `intervalCheck(): bool` | 周期检查空闲对象时 | 探活；`false` 或异常会清理对象 |
| `gc()` | 丢弃、缩容或销毁空闲对象时 | 关闭底层资源；应尽可能可靠、可重复调用 |

`gc()` 是业务清理回调，不是 PHP 的对象销毁操作。清理后调用者仍可能持有 PHP 对象引用，但不应再使用其底层资源。

不要只在 `objectRestore()` 中重置表面状态。数据库事务、Redis 的事务/订阅模式、HTTP 客户端的请求上下文等都需要按客户端语义恢复，否则会影响下一个借用者。

## 借用与归还

以下片段假设已经定义快速开始中的 `DemoPool` 并创建 `$pool`，且在协程内执行。

### getObj：手动管理

`getObj(?float $timeout = null, int $tryTimes = 3): ?ObjectInterface` 返回一个经过 `beforeUse()` 检查的对象。

```php
$object = $pool->getObj(0.5);
if ($object === null) {
    throw new RuntimeException('对象不可用');
}
try {
    $result = $object->execute();
} finally {
    $pool->recycleObj($object);
}
```

失败结果需要区分：

- Channel 获取失败（例如池耗尽后等待超时）抛出 `EasySwoole\Pool\Exception\PoolEmpty`，不是返回 `null`。
- `beforeUse()` 返回 `false` 且重试耗尽时，`getObj()` 可以返回 `null`；检查抛异常且耗尽重试时，异常向外传播。
- `createObject()` 异常直接向调用方传播，不受 `tryTimes` 控制。
- 池已销毁时抛出 `EasySwoole\Pool\Exception\Exception`。

`tryTimes` 控制借用前检查失败后的重试，不是建连失败重试策略。当前实现使用递归，且递归调用可能位于外层异常捕获范围内，不应把它当作所有混合失败场景的严格总尝试次数保证。

### recycleObj 与 unsetObj

`recycleObj($object): bool` 归还属于本池、已借出的对象，调用 `objectRestore()` 后放回队列。外来对象或已在池内的对象通常返回 `false`；恢复失败也可能返回 `false`，具体异常边界见后文。

`unsetObj($object): bool` 将属于本池、已借出的对象从登记中删除，调用 `gc()` 并释放名额。已在池内或不属于本池的对象返回 `false`。对象损坏、不应复用时使用它，之后不要再归还：

```php
$object = $pool->getObj();
if ($object !== null) {
    // 业务已判定该连接失效。
    $pool->unsetObj($object);
}
```

`isPoolObject($object)` 查询是否存在本池登记；`isInPool($object)` 查询登记中的在池状态。它们不是底层连接健康检查，也不是调用者所有权验证。

### invoke：回调结束时归还

```php
$result = $pool->invoke(function (DemoObject $object) {
    return $object->execute();
}, 0.5);
```

返回回调的返回值；正常结束或回调抛异常都会在 `finally` 中尝试归还。获取不到可用对象时抛 `PoolEmpty`。不要在回调内提前归还对象，也不要将对象本身返回后继续使用。

`invoke()` 每次从池借用，不会复用外层 `invoke()` 的对象。嵌套调用需要额外名额；容量为 1 时内层可能等待超时。对同一资源连续操作，应在一次回调中完成。

### defer：协程结束时归还

```php
$first = $pool->defer(0.5);
$second = $pool->defer();
assert($first === $second); // 同一池、同一协程复用同一个对象。
$first->execute();
// 此处没有自动归还；直到协程退出才归还。
```

同一池、同一协程重复 `defer()` 会直接返回缓存对象，不再次执行 `beforeUse()`，也不会再次应用超时。要同时借多个对象，应使用 `getObj()` 或 `invoke()`。

可以在当前协程主动 `recycleObj()` / `unsetObj()` 清除其 defer 缓存；再次 `defer()` 会重新获取。不要将 defer 对象交给另一协程归还：当前代码只按执行归还的协程 ID 清理上下文，原协程可能保留旧引用。

## 预热与健康检查

### keepMin

```php
$added = $pool->keepMin();  // 按 minObjectNum 补充。
$added = $pool->keepMin(10); // 补充到目标总数量，受 maxObjectNum 限制。
```

返回本次补充的数量，不是当前空闲数量。建连逐个执行，慢建连会拉长预热耗时；已借出的对象和创建中的名额也计入总数量。创建失败会报告错误并停止本次补充，不保证达到目标。当前 `keepMin(0)` 因宽松空值判断会使用配置中的最小数量；要不预热，应不调用此方法或将 `minObjectNum` 配为 `0`。

构造池不会立即建连。首次借用通常按需创建一个对象，不会自动一次性填满最小数量。服务需要预热时，在 worker 启动后的协程中调用 `keepMin()`；不要在 fork 前创建实际网络连接。

### 分批健康检查

池初始化后，定时器自动执行受保护的 `AbstractPool::intervalCheck()`，业务不能直接从池实例调用它。

- 每轮检查次数不超过“本轮开始时的空闲数量”和 `intervalCheckBatchSize` 中的较小值。
- 健康对象放回队尾，后续轮次继续检查其余对象；失效对象执行清理。
- 同一池的检查轮次不重叠；慢检查及补连接期间，新触发的轮次直接跳过。
- 检查后执行 `keepMin()`。批量上限只限制健康检查次数，不限制随后补连接的数量。
- 只检查空闲对象；已借出对象在下次获取时由 `beforeUse()` 检查。

默认每轮最多检查 8 个对象。无借用、缩容和失败干扰时，16 个空闲对象、5 秒周期大约需要两轮覆盖；这不是每个对象都在 5 秒内被检查的保证。检查会暂时取走资源；单个探活的超时应由业务客户端控制，批量数量限制不能中断卡住的检查。

## 自动缩容与状态

初始化后另有一个固定 5 秒周期的负载定时器，不受 `intervalCheckTime` 控制。它使用最近 15 个按秒分桶的 Channel 等待时间，除以成功借用次数；低于 `waitLoadAverageTime` 时尝试回收总数量的 10%（向下取整，至少 1 个），计划回收后需仍不少于 `minObjectNum`，且只能取走空闲对象。

将 `waitLoadAverageTime` 设为 `0` 可阻止当前实现的低负载缩容判断成立，但定时器仍会运行。没有成功借用时，平均值返回 `0`；只出现超时的区间不能据此判断负载很低。

```php
$state = $pool->status();
// ['createdNum' => 2, 'loadAverageTime' => 0.0]
$config = $pool->getConfig();
```

`status()` 当前只返回这两个字段：

- `createdNum`：登记/预占的总对象数量，包含借出、空闲及创建中的名额。
- `loadAverageTime`：平均 Channel 等待秒数，四舍五入到两位小数。不包含建连及 `beforeUse()` 时间，毫秒级等待可能显示为 `0.0`。

没有 `created`、`inuse`、`max`、`min` 等状态字段。最大/最小配置通过 `getConfig()` 读取；当前没有公开的空闲数、等待者数接口。

## 池管理器

`Manager` 在当前进程中保存池实例，不自动预热或创建业务连接。

```php
use EasySwoole\Pool\Manager;

$manager = Manager::getInstance();
$manager->register($pool, 'service');
$registered = $manager->get('service'); // 未注册返回 null。
if ($registered !== null) {
    $result = $registered->invoke(fn (DemoObject $object) => $object->execute());
}
```

省略名称时使用池的类名。同名注册会替换引用，不会销毁旧池；替换前应在没有活动借用时自行清理旧池，避免遗留定时器和连接。

**`Manager::resetAll()` 当前实际调用的是各池的 `destroy()`，不会恢复池可用状态，也不会删除注册记录。** 如需重新使用，取出池后调用其 `reset()`；不要把管理器方法和池方法的行为混淆。

## 销毁与重置

`destroy()` 标记池已销毁、停止定时器、清理队列中的空闲对象并关闭 Channel。未初始化池也可销毁，重复调用可用。销毁后 `getObj()`、`invoke()` 获取及 `recycleObj()` 会抛异常。

它不等待活动任务，也不会自动清理所有已借出对象。尤其 `defer()` 已有缓存时可能仍返回旧对象，退出回调再归还则会遇到已销毁异常。请先停止新任务、等待借用归还和检查完成，再销毁。

`reset()` 先销毁，再清空对象登记、defer 缓存和计数，允许下次获取时懒初始化。不会更换 `Config`，也不清空等待统计。不要在创建、检查或借用仍进行时重置；旧对象可能失去池归属登记，不能正常归还或通过 `unsetObj()` 清理。

```php
// 前提：没有活动借用、建连或健康检查。
$pool->reset();
$result = $pool->invoke(fn (DemoObject $object) => $object->execute());
$pool->destroy();
```

## 特别注意事项

### 超时不是整个操作的截止时间

`getObj($timeout)` 只将超时传给 Channel 的 `pop()`。对象创建、探活、恢复和关闭的时间由业务实现控制；检查失败后重试会再次使用同一个超时值。因此设置 1 秒不保证整个获取在 1 秒内结束。客户端应单独设置建连、读写和探活超时，业务另设总时间预算。

### 异常可能影响整个进程

- `createObject()` 异常经 `initObject()` 回滚创建名额后重新抛出，`getObj()` 不会替业务吞掉异常。
- `beforeUse()`、`intervalCheck()` 应返回严格的 `bool`，不要使用旧示例的 `?bool`。失败或异常会走清理逻辑，但清理本身也可能失败。
- `keepMin()`、健康检查、恢复和 `gc()` 的异常路径会使用 `trigger_error()`。当前不是一个保证不会抛异常的日志入口：若应用把 `E_USER_NOTICE` 转成异常，新异常可能逃出后台回调并导致进程退出。
- 尤其 `recycleObj()` 的异常分支先报告再清理；报告抛异常时，后续对象清理可能没有执行。不要把这些方法的 `bool` 返回类型理解为它们永远不会抛异常。
- `invoke()` 的 `finally` 归还或 defer 退出回调也可能抛异常；清理异常可能覆盖原业务异常。业务入口应捕获 `Throwable`，后台任务需结合应用错误处理策略设置最终异常边界。

默认错误处理器下，后台捕获并报告 Notice 通常不会中断进程。项目不保证任意错误处理器或任意对象回调下进程都能继续；不能仅依赖代码中的 `catch` 注释作判断。

### 所有权与容量

- 从 `getObj()` 借出的对象必须 `recycleObj()` 或 `unsetObj()`；仅 `unset($object)` 不会释放池名额。
- 归还后其他协程可能立即借用，同一对象不能继续操作、跨协程共享或重复归还。池检查登记状态，不跟踪当前合法借用者；陈旧引用可能操作其他任务正使用的对象。
- 对象登记目前使用 `spl_object_hash()`，只保存布尔状态。调用者丢弃借出对象又不注销时会留下登记；标识复用可能导致误认。更换为 `spl_object_id()` 本身不能解决生命周期问题。
- `defer()` 占用持续到协程结束，慢 HTTP 调用、任务等待等都会延长资源占用。常驻消费协程应按任务使用 `invoke()` 或主动归还。
- 对象数限制按池实例、按进程生效。多个 worker、多种连接名称需合并计算服务端连接预算；等待协程数没有独立上限，应在业务入口做并发控制。

### 初始化、配置和退出

运行中的配置对象是共享可变引用。修改容量不会重建 Channel；修改检查周期不会重新注册定时器。批量大小在每轮读取，其他选项不应假定动态修改都立即生效。要调整生命周期相关配置，应停止活动任务后重建或重置池。

池初始化后不能克隆；初始化前克隆也不会深拷贝 `Config`。业务建议显式构造不同池和配置实例。

独立脚本完成后要销毁已初始化的池，避免活动定时器继续运行。常驻服务按进程生命周期清理，不要在尚有并发借用时销毁或重置池。

## 运行测试

安装开发依赖后运行当前回归测试：

```bash
composer install
vendor/bin/phpunit --bootstrap vendor/autoload.php --do-not-cache-result tests/PoolRegressionTest.php
```

需要实际 Swoole 扩展。测试覆盖并发创建容量、创建失败释放名额、销毁、缩容、超时统计，以及健康检查批量上限、轮转、重叠和异常清理。`tests/Pool.php`、`tests/PoolObject.php` 是辅助对象，不是 PHPUnit 测试类。
