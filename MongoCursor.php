<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2017-11-15
 * Time: 11:40
 */
namespace SDF\Db;

class MongoCursor implements \Iterator{

    private $currenCursor = null;

    private $queryProcess = array();

    private $skip = 0;
    private $limit = 0;
    private $sort = array();

    private $mongoCollection;

    private $is_iterator;

    public function __construct($mongoCollection,$is_iterator=true){
        $this->mongoCollection = $mongoCollection;
        $this->is_iterator = $is_iterator;
        $this->skip = $mongoCollection->skip;
        $this->limit = $mongoCollection->limit;
        $this->sort = $mongoCollection->sort;
    }

    function rewind() {

        $this->currenCursor = $this->is_query(true);

        return $this->currenCursor->rewind();
    }

    function current() {
        $this->currenCursor = $this->is_query();

        return $this->mongoCollection->object_array($this->currenCursor->current());
    }

    function key() {
        $this->currenCursor = $this->is_query();

        return $this->currenCursor->key();
    }

    function next() {
        $this->currenCursor = $this->is_query();

        return $this->currenCursor->next();
    }

    function valid() {
        $this->currenCursor = $this->is_query();

        return $this->currenCursor->valid();
    }

    /**
     * 获取查询的游标对象
     * @param bool $is_iterator
     * @return mixed
     */
    public function is_query($is_new=false){
        $key_str =  md5(serialize($this->mongoCollection->filter).$this->mongoCollection->databaseName.$this->mongoCollection->collectionName);
        if($is_new || !isset($this->queryProcess[$key_str])){
            $cursor = $this->mongoCollection->findMany($this->mongoCollection->filter,$this->mongoCollection->projection,$this->limit,$this->skip,$this->sort);
            $this->queryProcess[$key_str] = $this->is_iterator ? new \IteratorIterator($cursor) : $cursor;
        }
        return $this->queryProcess[$key_str];
    }


    public function count(){
        return $this->mongoCollection->count($this->mongoCollection->filter);
    }
    public function sort($sort){
        $this->sort = $sort;
        return $this;
    }

    public function skip($skip){
        $this->skip = $skip;

        return $this;
    }

    public function limit($limit){
        $this->limit = $limit;
        return $this;
    }
}