<?php
/**
 * Created by PhpStorm.
 * User: yf
 * Date: 2019-01-06
 * Time: 22:48
 */

namespace EasySwoole\Pool\Tests;

use EasySwoole\Pool\ObjectInterface;

class PoolObject implements ObjectInterface
{
    protected $isOk = true;

    public function __construct()
    {
        var_dump('create');
    }


    function get()
    {
        return self::class;
    }

    function gc()
    {
        var_dump("gc");
    }

    function objectRestore()
    {
        var_dump('restore');
    }

    function beforeUse(): ?bool
    {
        var_dump('beforeUse');
        return $this->isOk;
    }

    /**
     * @return bool
     */
    public function isOk(): bool
    {
        return $this->isOk;
    }

    /**
     * @param bool $isOk
     */
    public function setIsOk(bool $isOk): void
    {
        $this->isOk = $isOk;
    }

}
