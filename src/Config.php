<?php


namespace EasySwoole\Pool;


use EasySwoole\Pool\Exception\Exception;
use EasySwoole\Spl\SplBean;

class Config extends SplBean
{
    protected int $intervalCheckTime = 5*1000;
    protected int $maxObjectNum = 16;
    protected int $intervalCheckBatchSize = 8;
    protected int $minObjectNum = 8;
    protected float $getObjectTimeout = 3.0;
    protected float $waitLoadAverageTime = 0.001;
    protected mixed $extraConf;


    public function getIntervalCheckTime(): int
    {
        return $this->intervalCheckTime;
    }

    public function setIntervalCheckTime($intervalCheckTime): Config
    {
        $this->intervalCheckTime = $intervalCheckTime;
        return $this;
    }


    public function getIntervalCheckBatchSize(): int
    {
        return $this->intervalCheckBatchSize;
    }

    public function setIntervalCheckBatchSize(int $intervalCheckBatchSize): Config
    {
        if ($intervalCheckBatchSize < 1) {
            throw new Exception('interval check batch size must be positive');
        }
        $this->intervalCheckBatchSize = $intervalCheckBatchSize;
        return $this;
    }

    public function getMaxObjectNum(): int
    {
        return $this->maxObjectNum;
    }

    public function setMaxObjectNum(int $maxObjectNum): Config
    {
        if($this->minObjectNum >= $maxObjectNum){
            throw new Exception('min num is bigger than max');
        }
        $this->maxObjectNum = $maxObjectNum;
        return $this;
    }

    public function getGetObjectTimeout(): float
    {
        return $this->getObjectTimeout;
    }


    public function setGetObjectTimeout(float $getObjectTimeout): Config
    {
        $this->getObjectTimeout = $getObjectTimeout;
        return $this;
    }

    public function getExtraConf():mixed
    {
        return $this->extraConf;
    }


    public function setExtraConf(mixed $extraConf): Config
    {
        $this->extraConf = $extraConf;
        return $this;
    }


    public function getMinObjectNum(): int
    {
        return $this->minObjectNum;
    }

    public function getWaitLoadAverageTime(): float
    {
        return $this->waitLoadAverageTime;
    }


    public function setWaitLoadAverageTime(float $waitLoadAverageTime): Config
    {
        $this->waitLoadAverageTime = $waitLoadAverageTime;
        return $this;
    }

    public function setMinObjectNum(int $minObjectNum): Config
    {
        if($minObjectNum >= $this->maxObjectNum){
            throw new Exception('min num is bigger than max');
        }
        $this->minObjectNum = $minObjectNum;
        return $this;
    }

    protected function initialize(): void
    {
        if ($this->intervalCheckBatchSize < 1) {
            throw new Exception('interval check batch size must be positive');
        }
        if($this->minObjectNum >= $this->maxObjectNum){
            throw new Exception('min num is bigger than max');
        }
    }
}