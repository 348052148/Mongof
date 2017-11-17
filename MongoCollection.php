<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2017-11-14
 * Time: 12:16
 */
namespace SDF\Db;

class MongoCollection extends MongoDB {
    private $currenCursor = null;


    public $skip = 0;
    public $limit = 0;
    public $sort = array();

    public $filter = array(); //过滤字符串
    public $queryOptions = array(); //查询选项
    public $projection = array();


    function object_array($array) {
        if(is_object($array)) {
            $array = (array)$array;
        } if(is_array($array)) {
            foreach($array as $key=>$value) {
                if(!is_a($value,'MongoDB\BSON\ObjectId')){
                    $array[$key] = $this->object_array($value);
                }
            }
        }
        return $array;
    }


    /**
     * 查询多条
     * @param array $filter
     * @param array $projection
     * @return $this
     */
    public function find($filter=array(), array $projection = [],$flag=true){
        $this->filter = $filter;
        $this->projection = $projection;
        return new MongoCursor($this,$flag);
    }

    /**
     * 查询单条记录
     * @param array $filter
     * @param array $projection
     * @return array
     */
    function findOne(array $filter = [], array $projection = []){

        $this->limit = 1;

        $this->currenCursor = $this->find($filter,$projection,false)->is_query();

        $document = $this->object_array(current($this->currenCursor->toArray()));

        return $document;
    }

    /**
     * 根据id查询一条记录
     * @param $_id
     * @param array $fields
     * @return array
     */
    public function findOneById($_id, $fields = array()) {
        return $this->findOne(array('_id'=>self::format($_id)),$fields);
    }

    /**
     * 查询并修改
     * @param array $query
     * @param array $update
     * @param array $fields
     * @param array $options
     */
    public function findAndModify (array $query, array $update = array(), array $fields = array(), array $options = NULL) {
        //$options['new'],$options['upsert'],$options['sort']
        return $this->object_array(parent::findOneAndUpdate($query,$update,$fields));
    }

    /**
     * 去重操作
     * @param $key
     * @param array $query
     */
    public function distinct($key, array $query = NULL) {
        return parent::distinctMany($key,$query);
    }


    /**
     * 删除数据
     * @param array $criteria
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function remove(array $criteria = array(), array $options = array()) {
        return new MongoResult(parent::deleteMany($criteria,$options));
    }

    /**
     * Update 操作
     * @param array $criteria
     * @param array $newobj
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function update(array $criteria , array $newobj, array $options = array()) {
        return new MongoResult(parent::updateMany($criteria,$newobj));
    }

    /**
     * 批量插入
     * @param array $a
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function batchInsert(array $a, array $options = array()) {
        return new MongoResult(parent::insertMany($a,$options));
    }

    /**
     * 插入一条数据
     * @param $a
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function insert(&$a, array $options = array()) {
        $result = parent::insertOne($a,$options);
        return new MongoResult($result);
    }

    /**
     * save 操作
     * @param $a
     * @param array $options
     * @return array|\MongoDB\Driver\WriteResult
     */
    public function save(&$a) {
        if(isset($a['_id'])){
            $data = $a;
            unset($data['_id']);
            return new MongoResult(parent::updateOne(array('_id'=>$a['_id']),$data));
        }
        return new MongoResult($this->insert($a));
    }


    /**
     * 聚合操作
     * @param array $pipeline
     * @param array $op
     * @param array $pipelineOperators
     */
    public function aggregate ( array $pipeline, array $op=array(), array $pipelineOperators=array() ) {

        return $this->object_array(parent::aggregateMany($pipeline,$op));
    }

    public function count($filter=[]){
        return parent::countMany($filter);
    }

}