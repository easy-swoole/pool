<?php

namespace EasySwoole\Pool\Tests;

use EasySwoole\Pool\AbstractPool;
use EasySwoole\Pool\Config;
use EasySwoole\Pool\ObjectInterface;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;

class RegressionObject implements ObjectInterface
{
    public int $gcCount = 0;
    public int $checkCount = 0;
    public $checkCallback = null;

    public function gc() { $this->gcCount++; }
    public function objectRestore() {}
    public function beforeUse(): bool { return true; }
    public function intervalCheck(): bool
    {
        $this->checkCount++;
        return $this->checkCallback ? ($this->checkCallback)() : true;
    }
}

class RegressionPool extends AbstractPool
{
    public int $attempts = 0;
    public function checkNow(): void { $this->intervalCheck(); }
    public bool $failNext = false;
    public bool $slow = false;

    protected function createObject(): ObjectInterface
    {
        $this->attempts++;
        if ($this->slow) {
            Coroutine::sleep(0.02);
        }
        if ($this->failNext) {
            $this->failNext = false;
            throw new \RuntimeException('creation failed');
        }
        return new RegressionObject();
    }
}

class PoolRegressionTest extends TestCase
{
    private function pool(int $interval = 10000): RegressionPool
    {
        return new RegressionPool(new Config([
            'minObjectNum' => 0,
            'maxObjectNum' => 2,
            'intervalCheckTime' => $interval,
        ]));
    }

    public function testConcurrentCreationRespectsCapacity(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool();
            $pool->slow = true;
            $done = new Channel(6);
            $errors = [];
            for ($i = 0; $i < 6; $i++) {
                Coroutine::create(function () use ($pool, $done, &$errors) {
                    try {
                        $object = $pool->getObj(1);
                        Coroutine::sleep(0.02);
                        $pool->recycleObj($object);
                    } catch (\Throwable $error) {
                        $errors[] = $error;
                    } finally {
                        $done->push(true);
                    }
                });
            }
            for ($i = 0; $i < 6; $i++) {
                $this->assertTrue($done->pop(2));
            }
            try {
                $this->assertSame([], $errors);
                $this->assertSame(2, $pool->attempts);
                $this->assertSame(2, $pool->status()['createdNum']);
            } finally {
                $pool->destroy();
            }
        });
    }

    public function testFailedCreationReleasesReservedCapacity(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool();
            try {
                $pool->failNext = true;
                try {
                    $pool->getObj();
                    $this->fail('Expected creation failure');
                } catch (\RuntimeException $error) {
                    $this->assertSame('creation failed', $error->getMessage());
                }
                $this->assertSame(0, $pool->status()['createdNum']);
                $first = $pool->getObj();
                $second = $pool->getObj();
                $this->assertNotSame($first, $second);
                $pool->recycleObj($first);
                $pool->recycleObj($second);
            } finally {
                $pool->destroy();
            }
        });
    }

    public function testDestroyCollectsIdleObjectsExactlyOnce(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool();
            $object = $pool->getObj();
            $pool->recycleObj($object);
            $pool->destroy();
            $pool->destroy();
            $this->assertSame(1, $object->gcCount);
            $this->assertFalse($pool->isPoolObject($object));
            $this->assertSame(0, $pool->status()['createdNum']);
            $pool->reset();
            $next = $pool->getObj();
            $pool->recycleObj($next);
            $pool->destroy();
            $this->assertSame(1, $next->gcCount);
        });
    }

    public function testDestroyBeforeInitializationAndWithoutIntervalTimer(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool();
            $pool->destroy();
            $pool->destroy();
            $this->assertSame(0, $pool->status()['createdNum']);
            $pool = $this->pool(0);
            $object = $pool->getObj();
            $pool->recycleObj($object);
            $pool->destroy();
            $this->assertSame(1, $object->gcCount);
        });
    }

    public function testLoadShrinkRemovesObjectRegistration(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool(0);
            try {
                $object = $pool->getObj();
                $pool->recycleObj($object);
                Coroutine::sleep(5.1);
                $this->assertSame(1, $object->gcCount);
                $this->assertFalse($pool->isPoolObject($object));
                $this->assertFalse($pool->isInPool($object));
                $this->assertSame(0, $pool->status()['createdNum']);
                $this->assertFalse($pool->recycleObj($object));
                $next = $pool->getObj();
                $this->assertNotSame($object, $next);
                $pool->recycleObj($next);
            } finally {
                $pool->destroy();
            }
            $this->assertSame(1, $object->gcCount);
        });
    }

    public function testTimeoutOnlyStatisticsDoNotEmitWarnings(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool(0);
            $first = $pool->getObj();
            $second = $pool->getObj();
            try {
                // Keep successful borrows in an earlier second than the timeout.
                Coroutine::sleep(1.1);
                try {
                    $pool->getObj(0.001);
                    $this->fail('Expected pool exhaustion');
                } catch (\EasySwoole\Pool\Exception\PoolEmpty $error) {
                    $this->assertInstanceOf(\EasySwoole\Pool\Exception\PoolEmpty::class, $error);
                }
                set_error_handler(function ($severity, $message, $file, $line) {
                    throw new \ErrorException($message, 0, $severity, $file, $line);
                });
                try {
                    $status = $pool->status();
                    $this->assertSame(2, $status['createdNum']);
                    $this->assertGreaterThanOrEqual(0, $status['loadAverageTime']);
                    $this->assertSame($status, $pool->status());
                } finally {
                    restore_error_handler();
                }
            } finally {
                $pool->recycleObj($first);
                $pool->recycleObj($second);
                $pool->destroy();
            }
        });
    }

    public function testHealthChecksAreBoundedAndRotate(): void
    {
        Coroutine\run(function () {
            $pool = new RegressionPool(new Config([
                'minObjectNum' => 0, 'maxObjectNum' => 5,
                'intervalCheckTime' => 0, 'intervalCheckBatchSize' => 2,
            ]));
            $objects = [];
            try {
                for ($i = 0; $i < 4; $i++) { $objects[] = $pool->getObj(); }
                foreach ($objects as $object) { $pool->recycleObj($object); }
                $pool->checkNow();
                $this->assertSame([1, 1, 0, 0], array_column($objects, 'checkCount'));
                $pool->checkNow();
                $this->assertSame([1, 1, 1, 1], array_column($objects, 'checkCount'));
                $pool->getConfig()->setIntervalCheckBatchSize(10);
                $pool->checkNow();
                $this->assertSame([2, 2, 2, 2], array_column($objects, 'checkCount'));
            } finally { $pool->destroy(); }
        });
    }

    public function testSlowHealthChecksDoNotOverlap(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool(0);
            $object = $pool->getObj();
            $entered = new Channel(1);
            $release = new Channel(1);
            $done = new Channel(1);
            $object->checkCallback = function () use ($entered, $release) {
                $entered->push(true);
                $release->pop();
                return true;
            };
            $pool->recycleObj($object);
            Coroutine::create(function () use ($pool, $done) {
                try { $pool->checkNow(); } finally { $done->push(true); }
            });
            try {
                $this->assertTrue($entered->pop(1));
                $pool->checkNow();
                $this->assertSame(1, $object->checkCount);
                $this->assertSame(1, $pool->attempts);
            } finally {
                $release->push(true);
                $this->assertTrue($done->pop(1));
                $object->checkCallback = null;
                $pool->checkNow();
                $this->assertSame(2, $object->checkCount);
                $pool->destroy();
            }
        });
    }

    public function testFailedHealthCheckIsCollectedAndGuardReleased(): void
    {
        Coroutine\run(function () {
            $pool = $this->pool(0);
            $object = $pool->getObj();
            $object->checkCallback = function () { throw new \RuntimeException('check failed'); };
            $pool->recycleObj($object);
            set_error_handler(function ($severity, $message) { return true; });
            try { $pool->checkNow(); } finally { restore_error_handler(); }
            try {
                $this->assertSame(1, $object->gcCount);
                $this->assertFalse($pool->isPoolObject($object));
                $next = $pool->getObj();
                $pool->recycleObj($next);
                $pool->checkNow();
                $this->assertSame(1, $next->checkCount);
            } finally { $pool->destroy(); }
        });
    }

    public function testHealthCheckBatchSizeMustBePositive(): void
    {
        foreach ([0, -1] as $invalid) {
            try {
                new Config(['intervalCheckBatchSize' => $invalid]);
                $this->fail('Expected invalid constructor configuration');
            } catch (\EasySwoole\Pool\Exception\Exception $error) {
                $this->assertSame('interval check batch size must be positive', $error->getMessage());
            }
            try {
                (new Config())->setIntervalCheckBatchSize($invalid);
                $this->fail('Expected invalid setter configuration');
            } catch (\EasySwoole\Pool\Exception\Exception $error) {
                $this->assertSame('interval check batch size must be positive', $error->getMessage());
            }
        }
    }

}
