<?PHP
namespace SDF\Db;

use MongoDB\BSON\ObjectId;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;

/**
 * Class MongoDb
 * @package SDF\Db
 */
 class MongoDB {
    protected static $Manager = NULL;//数据库管理类
    protected $mongo_config = array();//数据库连接配置
    public $databaseName = 'ismbao';//要操作的数据库
    public $collectionName = '';//要操作的集合
    protected $readPreference = NULL;//数据读取模式

    /**
     * 数据库操作类
     * MongoDb constructor.
     * @param string $collectionName 集合名
     * @param string $config_name 配置名
     */
    public function __construct($collectionName = '', $config_name = NULL){
        $mongo_config = \MongoDbConf::getConfig($config_name);
        $authSource = @$mongo_config['uriOptions']['authSource'];//权限验证数据库名
        $this->databaseName = empty($mongo_config['use_db']) ? $authSource : $mongo_config['use_db'];//需要连接处理的数据库
        $mongo_config['uriOptions'] = isset($mongo_config['uriOptions']) ? $mongo_config['uriOptions'] : array();

        $this->mongo_config = $mongo_config;
        if (empty($collectionName)) {
            $collectionName = basename(str_replace('\\', '/', get_class($this)));
        }
        $this->collectionName = $collectionName;

        if (!isset(self::$Manager)) {
            self::$Manager = new Manager($this->mongo_config['dsn'],$this->mongo_config['option']);
        }
        if(!empty($this->mongo_config['uriOptions']['ReadPreference'])){
            self::$Manager = self::$Manager->selectServer(new ReadPreference($this->mongo_config['uriOptions']['ReadPreference']));
        }
        $this->readPreference = self::$Manager->getReadPreference();
    }

    /**
     * 查询方法
     * @param $filter
     * @param array $queryOptions
     * @param ReadPreference|NULL $readPreference
     * @return \MongoDB\Driver\Cursor
     */
    public function query($filter, array $queryOptions = [],ReadPreference $readPreference = NULL){
        $query = new Query($filter, $queryOptions);
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName??'ismbao';
        $cursor = self::$Manager->executeQuery($databaseName . '.' . $collectionName, $query,$readPreference);
        return $cursor;
    }

     function handle_document(&$document){

         foreach($document as $doc_name=>$doc){

             if(is_array($doc)){
                 $this->handle_document($document[$doc_name]);
             }


             if(strpos($doc_name,'.')>0){
                 $doc_arr = explode('.',$doc_name);
                 unset($document[$doc_name]);
                 $doc_arr[0] = str_replace('$','',$doc_arr[0]);
                 $doc_arr[1] = str_replace('$','',$doc_arr[1]);
                 $document[$doc_arr[0]][$doc_arr[1]] = $doc;

                 if(is_array($doc)){
                     $this->handle_document($document[$doc_arr[0]][$doc_arr[1]]);
                 }
             }

             if(preg_match('/\$/',$doc_name)){
                 $document[str_replace('$','',$doc_name)] = $doc;
                 unset($document[$doc_name]);

             }

         }
     }

    /**
     * 返回数量
     * @param array $filter
     * @param ReadPreference|NULL $readPreference
     * @return int
     * @throws \Exception
     */
    public function countMany($filter = [],ReadPreference $readPreference = NULL){
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }
        $cmd = ['count' => $this->collectionName];
        $cmd['query'] = (object) $filter;
        $this->databaseName = $this->databaseName??'ismbao';
        $command = new Command($cmd);//mongodb各种命令详解：https://docs.mongodb.com/manual/reference/command/nav-crud/
        $cursor = self::$Manager->executeCommand($this->databaseName, $command,$readPreference);
        $result = current($cursor->toArray());
        // Older server versions may return a float
        if ( ! isset($result->n) || ! (is_integer($result->n) || is_float($result->n))) {
            throw new \Exception('count command did not return a numeric "n" value');
        }
        return (integer) $result->n;
    }

    /**
     * 在集合中找到指定字段的不同值。
     * @param $fieldName
     * @param array $filter
     * @param ReadPreference|NULL $readPreference
     * @return mixed
     * @throws \Exception
     */
    public function distinctMany($fieldName, $filter = [], ReadPreference $readPreference = NULL){
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }
        $cmd = [
            'distinct' => $this->collectionName,
            'key' => $fieldName,
        ];
        $cmd['query'] = (object) $filter;
        $command = new Command($cmd);
        $cursor = self::$Manager->executeCommand($this->databaseName, $command,$readPreference);
        $result = current($cursor->toArray());
        if ( ! isset($result->values) || ! is_array($result->values)) {
            throw new \Exception('distinct command did not return a "values" array');
        }
        return $result->values;
    }


    /**
     * 查询多个文档
     * @param $filter
     * @param array $projection
     * @param int $limit
     * @param int $skip
     * @param array $sort
     * @param ReadPreference|NULL $readPreference
     * @return \MongoDB\Driver\Cursor
     */
    public function findMany($filter=array(), array $projection = [],int $limit = 0,int $skip = 0,array $sort = [],ReadPreference $readPreference = NULL){
        $newproject = array();
        foreach($projection as $project){
            $newproject[$project] = 1;
        }
        $queryOptions = [
            'projection' => $newproject,
            'limit' => $limit,
            'skip' => $skip,
            'sort' => $sort
        ];

        $cursor = $this->query($filter,$queryOptions,$readPreference);

        return $cursor;
    }

    /**
     * 聚合查询
     */
    public function aggregateMany(array $pipeline, array $op=array(), ReadPreference $readPreference = NULL){
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }

        $cmd = [
            'aggregate' => $this->collectionName,
            'pipeline' => $pipeline
        ];

        $command = new Command($cmd);

        $this->databaseName = $this->databaseName ?? 'ismbao';

        $cursor = self::$Manager->executeCommand($this->databaseName, $command,$readPreference);


        $result = current($cursor->toArray());
        if ( ! isset($result->result) || ! is_array($result->result)) {
            throw new \Exception('aggregate command did not return a "values" array');
        }
        return $result;
    }

    /**
     * 插入一个文档
     * @param $document
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function insertOne(&$document, array $options = [],WriteConcern $writeConcern = NULL) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $this->handle_document($document);
        $insertId = $bulk->insert($document);
        if(!empty($insertId)) $document['_id'] = $insertId;
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName??'ismbao';
        $writeResult  = self::$Manager->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        //$insertedCount = $result->getInsertedCount();//返回插入的文档数量
        return $writeResult;
    }

    /**
     * @param $documents
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function insertMany(array &$documents,array $options = [],WriteConcern $writeConcern = NULL) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        foreach ($documents as &$document){
            $insertId = $bulk->insert($document);
            if(!empty($insertId)) $document['_id'] = $insertId;
        }
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName??'ismbao';
        $writeResult  = self::$Manager->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 修改一个文档
     * @param $filter
     * @param $newObj
     * @param bool $upsert  是否在没有匹配到修改文档时，插入数据。
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function updateOne($filter, $newObj ,bool $upsert = false,array $options = [],WriteConcern $writeConcern = NULL) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $updateOptions = array(
            'multi' => false,//只修改第一个匹配到的文档。
            'upsert' => $upsert,//是否在没有匹配到修改文档时，插入数据。
        );
        $bulk->update($filter,$newObj ,$updateOptions);
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName;
        $writeResult  = self::$Manager->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 修改多个文档
     * @param $filter
     * @param $newObj
     * @param bool $upsert
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function updateMany($filter, $newObj ,bool $upsert = false,array $options = [],WriteConcern $writeConcern = NULL) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $updateOptions = array(
            'multi' => true,//修改所有匹配到的文档
            'upsert' => $upsert,//是否在没有匹配到修改文档时，插入数据。
        );
        $bulk->update($filter,$newObj ,$updateOptions);
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName??'ismbao';
        $writeResult  = self::$Manager->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 删除一个文档
     * @param $filter
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function deleteOne($filter,array $options = [],WriteConcern $writeConcern = NULL) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $deleteOptions = array(
            'limit' => true,//是否删除所有匹配到的文档。
        );
        $bulk->delete($filter,$deleteOptions);
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName;
        $writeResult  = self::$Manager->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 删除匹配到的文档
     * @param $filter
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function deleteMany($filter,array $options = [],WriteConcern $writeConcern = NULL) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $deleteOptions = array(
            'limit' => false,//是否删除所有匹配到的文档。
        );
        $bulk->delete($filter,$deleteOptions);
        $collectionName = $this->collectionName;
        $databaseName = $this->databaseName ?? 'ismbao';
        $writeResult  = self::$Manager->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 查找并修改
     * @param array $filter
     * @param array $update
     * @param array $fields
     * @param bool $new
     * @param bool $upsert
     * @param array $sort
     * @param ReadPreference|NULL $readPreference
     * @return mixed
     * @throws \Exception
     */
    public function findOneAndUpdate(array $filter, array $update,array $fields = [],bool $new = false,bool $upsert = false,array $sort = [],ReadPreference $readPreference = NULL){
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }
        $cmd = array(
            'findAndModify' => $this->collectionName,//用于运行此命令的集合，命令详解：https://docs.mongodb.com/manual/reference/command/findAndModify/
            'update' => $update,//修改语句
            'new' => $new,//返回修改后的数据还是修改前的数据
            'query' => (object)$filter,//筛选条件
            'sort' => $sort,//对筛选结果排序
            'upsert' => $upsert,//没有匹配到文档时，插入。
            'fields' => $fields,//返回的字段
        );
        $command = new Command($cmd);

        $cursor = self::$Manager->executeCommand($this->databaseName, $command,$readPreference);
        $result = current($cursor->toArray());
        if ( ! isset($result->value)) {
            throw new \Exception('distinct command did not return a "value" array');
        }
        return $result->value;
    }

    /**
     * 获取自增id
     * @param string $collectionName 集合名
     * @return int
     */
    public function getNextId($collectionName = ''){
        if(empty($collectionName)){
            $collectionName = $this->collectionName;
        }
        $databaseName = $this->databaseName;
        //$collection_counters = new self('counters','local');
        $collection_counters = new class('counters','local') extends MongoDb {};
        $ret = $collection_counters->findOneAndUpdate(
            array('_id'=>$databaseName.'.'.$collectionName),
            array('$inc'=>array('seq'=>1)),
            [],
            true,
            true
        );
        return $ret->seq;
    }

    /**
     * 格式化id数据类型
     * @param $id
     * @return int|ObjectId|string
     */
    public static function format($id){
        if(is_int($id)){
            return intval($id);
        }elseif(is_object($id)){
            return $id;
        }else{
            if(strlen($id)<=24){
                $id = sprintf("%024s",$id);
                return  new ObjectId($id);
            }else{
                return $id;
            }
        }
    }

    /**
     * Return the collection namespace (e.g. "db.collection").
     *
     * @see https://docs.mongodb.org/manual/faq/developers/#faq-dev-namespace
     * @return string
     */
    public function __toString() {
        return $this->databaseName . '.' . $this->collectionName;
    }

     public static function iterator_to_array($cursor){
        $data = array();
         foreach($cursor as $c){
             $data[] = $c;
         }
         return $data;
     }
}