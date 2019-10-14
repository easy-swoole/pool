<?php


namespace EasySwoole\Pool;


use EasySwoole\Pool\Exception\Exception;
use EasySwoole\Pool\Exception\PoolEmpty;
use EasySwoole\Utility\Random;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Timer;

abstract class AbstractPool
{
    private $createdNum = 0;
    private $poolChannel;
    private $objHash = [];
    /** @var Config  */
    private $conf;
    private $timerId;
    private $destroy = false;
    private $context = [];

    /*
     * 如果成功创建了,请返回对应的obj
     */
    abstract protected function createObject();

    public function __construct(Config $conf)
    {
        if ($conf->getMinObjectNum() >= $conf->getMaxObjectNum()) {
            $class = static::class;
            throw new Exception("pool max num is small than min num for {$class} error");
        }
        $this->conf = $conf;
        $this->poolChannel = new Channel($conf->getMaxObjectNum() + 8);
        if ($conf->getIntervalCheckTime() > 0) {
            $this->timerId = Timer::tick($conf->getIntervalCheckTime(), [$this, 'intervalCheck']);
        }
    }

    /*
     * 回收一个对象
     */
    public function recycleObj($obj): bool
    {
        /*
         * 当标记为销毁后，直接进行对象销毁
         */
        if($this->destroy){
            $this->unsetObj($obj);
            return true;
        }
        /*
         * 仅仅允许归属于本pool且不在pool内的对象进行回收
         */
        if($this->isPoolObject($obj) && (!$this->isInPool($obj))){
            /*
             * 主动回收可能存在的上下文
            */
            $cid = Coroutine::getCid();
            if(isset($this->context[$cid])){
                unset($this->context[$cid]);
            }
            $hash = $obj->__objHash;
            //标记为在pool内
            $this->objHash[$hash] = true;
            if($obj instanceof ObjectInterface){
                try{
                    $obj->objectRestore();
                }catch (\Throwable $throwable){
                    //重新标记为非在pool状态,允许进行unset
                    $this->objHash[$hash] = false;
                    $this->unsetObj($obj);
                    throw $throwable;
                }
            }
            $this->poolChannel->push($obj);
            return true;
        }else{
            return false;
        }
    }

    /*
     * tryTimes为出现异常尝试次数
     */
    public function getObj(float $timeout = null, int $tryTimes = 3)
    {
        /*
         * 当标记为销毁后，禁止取出对象
         */
        if($this->destroy){
            return null;
        }
        if($timeout === null){
            $timeout = $this->getConfig()->getGetObjectTimeout();
        }
        $object = null;
        if($this->poolChannel->isEmpty()){
            try{
                $this->initObject();
            }catch (\Throwable $throwable){
                if($tryTimes <= 0){
                    throw $throwable;
                }else{
                    $tryTimes--;
                    return $this->getObj($timeout,$tryTimes);
                }
            }
        }
        $object = $this->poolChannel->pop($timeout);
        if(is_object($object)){
            if($object instanceof ObjectInterface){
                try{
                    if($object->beforeUse() === false){
                        $this->unsetObj($object);
                        if($tryTimes <= 0){
                            return null;
                        }else{
                            $tryTimes--;
                            return $this->getObj($timeout,$tryTimes);
                        }
                    }
                }catch (\Throwable $throwable){
                    $this->unsetObj($object);
                    if($tryTimes <= 0){
                        throw $throwable;
                    }else{
                        $tryTimes--;
                        return $this->getObj($timeout,$tryTimes);
                    }
                }
            }
            $hash = $object->__objHash;
            //标记该对象已经被使用，不在pool中
            $this->objHash[$hash] = false;
            $object->__lastUseTime = time();
            return $object;
        }else{
            return null;
        }
    }

    /*
     * 彻底释放一个对象
     */
    public function unsetObj($obj): bool
    {
        if($this->isPoolObject($obj) && (!$this->isInPool($obj))){
            /*
             * 主动回收可能存在的上下文
             */
            $cid = Coroutine::getCid();
            if(isset($this->context[$cid])){
                unset($this->context[$cid]);
            }
            $hash = $obj->__objHash;
            unset($this->objHash[$hash]);
            if($obj instanceof ObjectInterface){
                try{
                    $obj->gc();
                }catch (\Throwable $throwable){
                    throw $throwable;
                }finally{
                    $this->createdNum--;
                }
            }else{
                $this->createdNum--;
            }
            return true;
        }else{
            return false;
        }
    }

    /*
     * 超过$idleTime未出队使用的，将会被回收。
     */
    public function idleCheck(int $idleTime)
    {
        $list = [];
        while (!$this->poolChannel->isEmpty()){
            $item = $this->poolChannel->pop(0.01);
            if(time() - $item->__lastUseTime > $idleTime){
                //标记为不在队列内，允许进行gc回收
                $hash = $item->__objHash;
                $this->objHash[$hash] = false;
                $this->unsetObj($item);
            }else{
                $list[] = $item;
            }
        }
        foreach ($list as $item){
            $this->poolChannel->push($item);
        }
    }

    /*
     * 允许外部调用
     */
    public function intervalCheck()
    {
        $this->idleCheck($this->getConfig()->getMaxIdleTime());
        $this->keepMin($this->getConfig()->getMinObjectNum());
    }

    public function keepMin(?int $num = null): int
    {
        if($this->createdNum < $num){
            $left = $num - $this->createdNum;
            while ($left > 0 ){
                $this->initObject();
                $left--;
            }
        }
        return $this->createdNum;
    }

    /*
     * 用以解决冷启动问题,其实是是keepMin别名
    */
    public function preLoad(?int $num = null): int
    {
        return $this->keepMin($num);
    }



    public function getConfig():Config
    {
        return $this->conf;
    }

    public function status()
    {
        return [
            'created' => $this->createdNum,
            'inuse' => $this->createdNum - $this->poolChannel->stats()['queue_num'],
            'max' => $this->getConfig()->getMaxObjectNum(),
            'min' => $this->getConfig()->getMinObjectNum()
        ];
    }

    private function initObject():bool
    {
        $obj = null;
        $this->createdNum++;
        if($this->createdNum > $this->getConfig()->getMaxObjectNum()){
            $this->createdNum--;
            return false;
        }
        try{
            $obj = $this->createObject();
            if(is_object($obj)){
                $hash = Random::character(12);
                $this->objHash[$hash] = true;
                $obj->__objHash = $hash;
                $obj->__lastUseTime = time();
                $this->poolChannel->push($obj);
                return true;
            }else{
                $this->createdNum--;
            }
        }catch (\Throwable $throwable){
            $this->createdNum--;
            throw $throwable;
        }
        return false;
    }

    public function isPoolObject($obj):bool
    {
        if(isset($obj->__objHash)){
            return isset($this->objHash[$obj->__objHash]);
        }else{
            return false;
        }
    }

    public function isInPool($obj):bool
    {
        if($this->isPoolObject($obj)){
            return $this->objHash[$obj->__objHash];
        }else{
            return false;
        }
    }

    function destroyPool()
    {
        $this->destroy = true;
        if($this->timerId && Timer::exists($this->timerId)){
            Timer::clear($this->timerId);
            $this->timerId = null;
        }
        while (!$this->poolChannel->isEmpty()){
            $item = $this->poolChannel->pop(0.01);
            $this->unsetObj($item);
        }
    }

    public function invoke(callable $call,float $timeout = null)
    {
        $obj = $this->getObj($timeout);
        if($obj){
            try{
                $ret = call_user_func($call,$obj);
                return $ret;
            }catch (\Throwable $throwable){
                throw $throwable;
            }finally{
                $this->recycleObj($obj);
            }
        }else{
            throw new PoolEmpty(static::class." pool is empty");
        }
    }

    public function defer(float $timeout = null)
    {
        $cid = Coroutine::getCid();
        if(isset($this->context[$cid])){
            return $this->context[$cid];
        }
        $obj = $this->getObj($timeout);
        if($obj){
            $this->context[$cid] = $obj;
            Coroutine::defer(function ()use($cid){
               if(isset($this->context[$cid])){
                   $obj = $this->context[$cid];
                   unset($this->context[$cid]);
                   $this->recycleObj($obj);
               }
            });
            return $this->defer($timeout);
        }else{
            throw new PoolEmpty(static::class." pool is empty");
        }
    }
}