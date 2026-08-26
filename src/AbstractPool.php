<?php


namespace EasySwoole\Pool;


use EasySwoole\Pool\Exception\Exception;
use EasySwoole\Pool\Exception\PoolEmpty;
use EasySwoole\Pool\Tests\PoolObject;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Timer;

abstract class AbstractPool
{
    private int $createdNum = 0;

    private Channel|null $poolChannel = null;
    private array $objHashInPool = [];
    private Config $conf;
    private int|null $intervalCheckTimerId;
    private int|null $loadAverageTimerId;
    private bool $destroy = false;
    private array $deferContextObj = [];
    private array $getObjWaitTimeInfo = [];
    private array $objectUseTimesInfo = [];


    /*
     * 如果成功创建了,请返回对应的obj
     */
    abstract protected function createObject():ObjectInterface;

    public function __construct(?Config $conf = null)
    {
        if($conf == null){
            $conf = new Config();
        }
        if ($conf->getMinObjectNum() >= $conf->getMaxObjectNum()) {
            $class = static::class;
            throw new Exception("pool max num is small than min num for {$class} error");
        }
        $this->conf = $conf;
    }

    /*
     * tryTimes为出现异常尝试次数
     */
    public function getObj(float|null $timeout = null, int $tryTimes = 3):?PoolObject
    {
        if ($this->destroy) {
            throw new Exception("pool is already destroyed");
        }

        $this->init();

        if ($timeout === null) {
            $timeout = $this->getConfig()->getGetObjectTimeout();
        }

        if($this->poolChannel->isEmpty()){
            $this->initObject();
        }

        //记录pop等待耗时
        $start = microtime(true);
        $object = $this->poolChannel->pop($timeout);
        $timeKey = time();
        $takeTime = microtime(true) - $start;
        if(isset($this->getObjWaitTimeInfo[$timeKey])){
            $this->getObjWaitTimeInfo[$timeKey] += $takeTime;
        }else{
            $this->getObjWaitTimeInfo[$timeKey] = $takeTime;
        }

        if(empty($object)){
            throw new PoolEmpty();
        }

        $hash = spl_object_hash($object);
        //标记该对象已经被使用，不在pool中
        $this->objHashInPool[$hash] = false;
        try {
            if ($object->beforeUse() === false) {
                $this->unsetObj($object);
                if ($tryTimes <= 0) {
                    return null;
                } else {
                    $tryTimes--;
                    return $this->getObj($timeout, $tryTimes);
                }
            }
        } catch (\Throwable $throwable) {
            $this->unsetObj($object);
            if ($tryTimes <= 0) {
                throw $throwable;
            } else {
                $tryTimes--;
                return $this->getObj($timeout, $tryTimes);
            }
        }

        if(isset($this->objectUseTimesInfo[$timeKey])){
            $this->objectUseTimesInfo[$timeKey] += 1;
        }else{
            $this->objectUseTimesInfo[$timeKey] = 1;
        }

        return $object;
    }

    /*
     * 回收一个对象
     */
    public function recycleObj($obj): bool
    {
        /*
         * 当标记为销毁后，直接进行对象销毁
         */
        if ($this->destroy) {
            throw new Exception("pool is already destroyed");
        }
        /*
        * 懒惰模式，可以提前创建 pool对象，因此调用钱执行初始化检测
        */
        $this->init();
        /*
         * 仅仅允许归属于本pool且不在pool内的对象进行回收
         */
        if ($this->isPoolObject($obj) && (!$this->isInPool($obj))) {
            $hash = spl_object_hash($obj);
            /*
             * 主动回收可能存在的上下文
            */
            $cid = Coroutine::getCid();
            //因为该object不一定是defer出去的
            if (isset($this->deferContextObj[$cid]) && (spl_object_hash($this->deferContextObj[$cid]) === $hash)) {
                unset($this->deferContextObj[$cid]);
            }
            //标记为在pool内
            $this->objHashInPool[$hash] = true;
            try {
                $obj->objectRestore();
                $this->poolChannel->push($obj);
                return true;
            } catch (\Throwable $throwable) {
                //重新标记为非在pool状态,允许进行unset
                $this->objHashInPool[$hash] = false;
                $this->unsetObj($obj);
                throw $throwable;
            }
        } else {
            return false;
        }
    }



    /*
     * 彻底释放一个对象
     */
    public function unsetObj($obj): bool
    {
        if (!$this->isInPool($obj)) {
            $hash = spl_object_hash($obj);
            /*
             * 主动回收可能存在的上下文
             */
            $cid = Coroutine::getCid();
            //因为该object不一定是defer出去的
            if (isset($this->deferContextObj[$cid]) && (spl_object_hash($this->deferContextObj[$cid]) === $hash)) {
                unset($this->deferContextObj[$cid]);
            }

            unset($this->objHashInPool[$hash]);

            try {
                $obj->gc();
            } catch (\Throwable $throwable) {
                throw $throwable;
            } finally {
                $this->createdNum--;
            }
            return true;
        } else {
            return false;
        }
    }


    protected function intervalCheck(bool $throwError = false): void
    {
        try {
            $size = $this->poolChannel->length();
            while (!$this->poolChannel->isEmpty() && $size >= 0) {
                $size--;
                /** @var ObjectInterface $item */
                $item = $this->poolChannel->pop(0.01);
                if(!$item){
                    continue;
                }
                try{
                    if(!$item->intervalCheck()){
                        //标记为不在队列内，允许进行gc回收
                        $hash = spl_object_hash($item);
                        $this->objHashInPool[$hash] = false;
                        $this->unsetObj($item);
                    }else{
                        $this->poolChannel->push($item);
                    }
                }catch (\Throwable $throwable){
                    $hash = spl_object_hash($item);
                    $this->objHashInPool[$hash] = false;
                    $this->unsetObj($item);
                    if($throwError){
                        throw $throwable;
                    }else{
                        trigger_error($throwable->getMessage());
                    }
                }
            }
            $this->keepMin();
        }catch (\Throwable $throwable){
            //屏蔽此处产生的异常。避免因为定时器中未捕获的异常导致进程退出
            if($throwError){
                throw $throwable;
            }else{
                trigger_error($throwable->getMessage());
            }
        }
    }

    /*
    * 可以解决冷启动问题
    */
    public function keepMin(?int $num = null): int
    {
        $currentAdd = 0;
        if($num == null){
            $num = $this->getConfig()->getMinObjectNum();
        }
        if ($this->createdNum < $num) {
            $left = $num - $this->createdNum;
            while ($left > 1) {
                if (!$this->initObject()) {
                    break;
                }
                $left--;
                $currentAdd++;
            }
        }
        return $currentAdd;
    }


    public function getConfig(): Config
    {
        return $this->conf;
    }

    private function initObject(): ObjectInterface|null
    {
        if ($this->destroy) {
            return null;
        }
        /*
        * 懒惰模式，可以提前创建 pool对象，因此调用钱执行初始化检测
        */
        $this->init();

        if ($this->createdNum >= $this->getConfig()->getMaxObjectNum()) {
            return null;
        }
        $this->createdNum++;
        try {
            $obj = $this->createObject();
            $hash = spl_object_hash($obj);
            $this->objHashInPool[$hash] = true;
            $this->poolChannel->push($obj);
            return $obj;
        } catch (\Throwable $throwable) {
            $this->createdNum--;
            throw $throwable;
        }
    }

    public function isPoolObject($obj): bool
    {
        $hash = spl_object_hash($obj);
        return isset($this->objHashInPool[$hash]);
    }

    public function isInPool($obj): bool
    {
        if ($this->isPoolObject($obj)) {
            $hash = spl_object_hash($obj);
            return $this->objHashInPool[$hash];
        } else {
            return false;
        }
    }

    /*
     * 销毁该pool，但保留pool原有状态
     */
    function destroy(): void
    {
        $this->destroy = true;
        /*
        * 懒惰模式，可以提前创建 pool对象，因此调用钱执行初始化检测
        */
        $this->init();
        if ($this->intervalCheckTimerId && Timer::exists($this->intervalCheckTimerId)) {
            Timer::clear($this->intervalCheckTimerId);
            $this->intervalCheckTimerId = null;
        }
        if ($this->loadAverageTimerId && Timer::exists($this->loadAverageTimerId)) {
            Timer::clear($this->loadAverageTimerId);
            $this->loadAverageTimerId = null;
        }

        if($this->poolChannel){
            while (!$this->poolChannel->isEmpty()) {
                $item = $this->poolChannel->pop(0.01);
                $this->unsetObj($item);
            }

            $this->poolChannel->close();
            $this->poolChannel = null;
        }
    }

    function reset(): AbstractPool
    {
        $this->destroy();
        $this->createdNum = 0;
        $this->destroy = false;
        $this->deferContextObj = [];
        $this->objHashInPool = [];
        return $this;
    }

    public function invoke(callable $call, float|null $timeout = null)
    {
        $obj = $this->getObj($timeout);
        if ($obj) {
            try {
                return call_user_func($call, $obj);
            } catch (\Throwable $throwable) {
                throw $throwable;
            } finally {
                $this->recycleObj($obj);
            }
        } else {
            throw new PoolEmpty( "pool is empty");
        }
    }

    public function defer(float|null $timeout = null):ObjectInterface
    {
        $cid = Coroutine::getCid();
        if (isset($this->deferContextObj[$cid])) {
            return $this->deferContextObj[$cid];
        }
        $obj = $this->getObj($timeout);
        if ($obj) {
            $this->deferContextObj[$cid] = $obj;
            Coroutine::defer(function () use ($cid) {
                if (isset($this->deferContextObj[$cid])) {
                    $obj = $this->deferContextObj[$cid];
                    unset($this->deferContextObj[$cid]);
                    $this->recycleObj($obj);
                }
            });
            return $this->defer($timeout);
        } else {
            throw new PoolEmpty( "pool is empty");
        }
    }

    private function init(): void
    {
        if ((!$this->poolChannel) && (!$this->destroy)) {
            $this->poolChannel = new Channel($this->conf->getMaxObjectNum() + 8);
            if ($this->conf->getIntervalCheckTime() > 0) {
                $this->intervalCheckTimerId = Timer::tick($this->conf->getIntervalCheckTime(),function (){
                    $this->intervalCheck();
                });
            }
            $this->loadAverageTimerId = Timer::tick(5*1000,function (){
                $currentKey = time();
                $getObjWaitTimeInfo = [];
                $getObjWaitTime = 0;
                $objectUseTimesInfo = [];
                $objectUseTimes = 0;
                $index = 0;
                while ($index < 15) {
                    if(isset($this->getObjWaitTimeInfo[$currentKey])){
                        $getObjWaitTimeInfo[$currentKey] = $this->getObjWaitTimeInfo[$currentKey];
                        $getObjWaitTime += $getObjWaitTimeInfo[$currentKey];
                        $objectUseTimesInfo[$currentKey] = $this->objectUseTimesInfo[$currentKey];
                        $objectUseTimes += $objectUseTimesInfo[$currentKey];
                    }
                    $currentKey--;
                    $index++;
                }
                $this->getObjWaitTimeInfo = $getObjWaitTimeInfo;
                $this->objectUseTimesInfo = $objectUseTimesInfo;

                $average = 0;
                if($objectUseTimes > 0){
                    $average = $getObjWaitTime / $objectUseTimes;
                }

                if($this->getConfig()->getWaitLoadAverageTime() > $average){
                    //负载小。尝试回收链接百分之5的链接
                    $decNum = intval($this->createdNum * 0.05);
                    if( ($this->createdNum - $decNum) > $this->getConfig()->getMinObjectNum()){
                        while ($decNum > 0){
                            $temp = $this->getObj(0.001,0);
                            if($temp){
                                $this->unsetObj($temp);
                            }else{
                                break;
                            }
                            $decNum--;
                        }
                    }
                }
            });
        }
    }

    final function __clone()
    {
        if($this->poolChannel){
            throw new Exception('pool cannot clone after init() call');
        }
    }
}
