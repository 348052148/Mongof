<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2017-11-16
 * Time: 14:30
 */
namespace SDF\Db;

class MongoResult implements \ArrayAccess{

    private $result;


    private $testData;


    public function __construct($result){
        $this->result = $result;

        $this->testData['nInserted'] = $this->result->getInsertedCount();
        $this->testData['nMatched'] = $this->result->getMatchedCount();
        $this->testData['nModified'] = $this->result->getModifiedCount();
        $this->testData['nRemoved'] = $this->result->getDeletedCount();
        $this->testData['nUpserted'] = $this->result->getUpsertedCount();

        $this->testData['ok'] = 0;

        $this->testData['n'] = 0;

        if($this->testData['nInserted']!=0){
            $this->testData['ok'] = 1;
            $this->testData['n'] = $this->testData['nInserted'];
        }

        if($this->testData['nMatched']!=0){
            $this->testData['ok'] = 1;
            $this->testData['n'] = $this->testData['nMatched'];
        }

        if($this->testData['nModified']!=0){
            $this->testData['ok'] = 1;
            $this->testData['n'] = $this->testData['nModified'];
        }

        if($this->testData['nRemoved']!=0){
            $this->testData['ok'] = 1;
            $this->testData['n'] = $this->testData['nRemoved'];
        }

    }

    public function offsetExists($key){
        return isset($this->testData[$key]);
    }

    public function offsetSet($key,$value){
        $this->testData[$key] = $value;
    }

    public function offsetGet($key){
        return $this->testData[$key];
    }

    public function offsetUnset($key){
        unset($this->testData[$key]);
    }
}