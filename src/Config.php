<?php


namespace EasySwoole\Pool;


use EasySwoole\Pool\Exception\Exception;
use EasySwoole\Spl\SplBean;

class Config extends SplBean
{
    protected int $intervalCheckTime = 10*1000;
    protected int $maxIdleTime = 15;
    protected int $maxObjectNum = 20;
    protected int $minObjectNum = 5;
    protected float $getObjectTimeout = 3.0;
    protected float $loadAverageTime = 0.001;
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


    public function getMaxIdleTime(): int
    {
        return $this->maxIdleTime;
    }


    public function setMaxIdleTime(int $maxIdleTime): Config
    {
        $this->maxIdleTime = $maxIdleTime;
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

    public function getLoadAverageTime(): float
    {
        return $this->loadAverageTime;
    }


    public function setLoadAverageTime(float $loadAverageTime): Config
    {
        $this->loadAverageTime = $loadAverageTime;
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
}