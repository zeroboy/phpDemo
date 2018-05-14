<?php
namespace Pelasticsearch;
require 'vendor/autoload.php';
use Elasticsearch\ClientBuilder as EsClient;
use Curl\Curl ;

class Pelasticsearch
{

	protected static $ins;
	private static $index;
	private static $type;
	private static $stime;
	private static $etime;
	private static $esclient;
    	private static $ES_HOSTS = ['0.0.0.0'];



    	CONST ES_HOST = '0.0.0.0';
	CONST ES_CLUSTER = ["0.0.0.1","0.0.0.2","0.0.0.3"];
	CONST ES_PORT = 9200;
	CONST ES_MAX_RESULT_WINDOW = 10000000;


	public static function Getins()
    {

		if(self::$ins === null){
			self::$ins = new self();
		}

		return self::$ins;
	}

	protected function __construct()
    {
	}

	protected function _clone()
    {
	}

    /**
     * 参数初始化、时间范围检测、查询索引ES_MAX_RESULT_WINDOW检测
     * @param string $index 索引
     * @param string $type  类型
     * @param int $stime    查询起始时间
     * @param int $etime    查询结束时间
     * @return array
     */
    public function init($index='', $type= '', $stime= 0, $etime= 0)
    {

        self::$index = $index;
        self::$type = $type;
        self::$stime = strtotime($stime);
        self::$etime = strtotime($etime);

        if(self::$stime && self::$etime){
            if(self::$stime>=self::$etime){
                return ['result'=>false,"msg"=>'查询时间错误'];
            }

            if((self::$etime-self::$stime)>=60*24*3600){
                return ['result'=>false,"msg"=>'查询时间间隔不能超过60天！'];
            }
        }

        try{
            //client
            //self::ESCLUSTER;
            self::$esclient = EsClient::create()->setHosts(self::$ES_HOSTS)->build();
            //self::$esclient = Elasticsearch\ClientBuilder::create()->setHosts(self::ES_HOST)->build();


            //初始化 max_result_window
            $curl = new Curl();

            $setting_url = "http://" . self::ES_HOST . ":" . self::ES_PORT . "/" . self::$index . "/_settings";
            $settinginfo_json = $curl->get($setting_url);
            $settinginfo_arr = json_decode(json_encode($settinginfo_json),true);

            if(!array_key_exists('max_result_window', $settinginfo_arr[self::$index]['settings']['index'])){

                $curl->setOpt(CURLOPT_HTTPHEADER, array('Content-type:application/json'));
                //return $curl;
                $json  = json_encode([
                    "index"=>[
                        "max_result_window"=>self::ES_MAX_RESULT_WINDOW
                    ]
                ]);
                $setresult = $curl->put($setting_url, $json);

                $setresult_arr = json_decode(json_encode($setresult),true);
                if(!$setresult_arr['acknowledged']){
                    return ['result'=>false, "msg"=>'setting max_result_window was error!'];
                }
                return ['result'=>true, "msg"=>$setresult_arr];
            }

        }catch (\Exception $e) {
            return ['result'=>false,'msg'=>$e->getMessage()];
        }

    }


    /**
     * 单查询
     * @param int $offset 偏移量
     * @param int $limit  长度
     * @return mixed
     */
    public function simpleQuery($offset=0,$limit=1){

        $params = [
            'index'=>self::$index,
            'type'=>self::$type,
            'body'=>[
                'from'=>$offset,
                'size'=>$limit
            ]
        ];

        (self::$stime && self::$etime) && ($params['body']['query']["bool"]["filter"]["range"]["ctime"] = ["gte"=>self::$stime, "lte"=>self::$etime]);

        return  self::$esclient->search($params);
    }

    /**
     * 符合条件查询
     * @param int $offset 偏移量
     * @param int $limit  长度
     * @param string $groupOp (AND|OR) 多条件逻辑关系
     * @param array $querydata ['op'=>'eq','field'=>'xxx','data'=>'xxx'] [运算符|字段|字段值]
     * @return mixed
     */
    public function multipleQuery($offset=0,$limit=1,$groupOp='',$querydata=[]){

        $params = [
            'index'=>self::$index,
            'body'=>[
                'from'=>$offset,
                'size'=>$limit
            ]
        ];

        (self::$stime && self::$etime) && ($params['body']['query']["bool"]["filter"]["range"]["ctime"] = ["gte"=>self::$stime, "lte"=>self::$etime]);

        if($groupOp && $querydata){

            foreach ($querydata as $k=>$v){

                switch ($groupOp){
                    case 'AND':
                        switch ($v['op']){
                            case 'eq'://相等
                                $params['body']['query']["bool"]["must"][]["match"] = [$v['field']=>$v['data']];
                                break;
                            case 'ne'://不等
                                $params['body']['query']["bool"]["must_not"][]["match"] = [$v['field']=>$v['data']];
                                break;
                            case 'gt'://大于
                                $params['body']['query']["bool"]["must"][]["range"][$v['field']] = ["gt"=>intval($v['data'])];
                                break;
                            case 'lt'://小于
                                $params['body']['query']["bool"]["must"][]["range"][$v['field']] = ["lt"=>intval($v['data'])];
                                break;
                            case 'bw'://开始于
                                $params['body']['query']["bool"]["must"][]['range'][$v['field']] = [
                                    "gte"=>$v['data']
                                ];
                                break;
                            case 'bn'://不开始于
                                $params['body']['query']["bool"]["must"][]['range'][$v['field']] = [
                                    "lt"=>$v['data']
                                ];
                                break;
                            case 'ew'://结束于
                                $params['body']['query']["bool"]["must"][]['range'][$v['field']] = [
                                    "lte"=>$v['data']
                                ];
                                break;
                            case 'nn'://非空
                                $params['body']['query']["bool"]["must"][]["exists"] = [
                                    "field"=>$v['field']
                                ];
                                break;
                            case 'nu'://空值
                                $params['body']['query']["bool"]["must_not"][]["exists"] = [
                                    "field"=>$v['field']
                                ];
                                break;
                            case 'en'://不结束于
                                $params['body']['query']["bool"]["must"][]['range'][$v['field']] = [
                                    "gt"=>$v['data']
                                ];
                                break;
                            case 'cn'://包含
                                $params['body']['query']["bool"]["must"][]['query_string'] = [
                                    'default_field' => $v['field'],
                                    'query' => $v['data']
                                ];
                                break;
                            case 'nc'://不包含
                                $params['body']['query']["bool"]["must_not"][]['query_string'] = [
                                    "gte"=>$v['data']
                                ];
                                break;
                            case 'in'://包含
                                $params['body']['query']["bool"]["must"][]['query_string'] = [
                                    'default_field' => $v['field'],
                                    'query' => $v['data']
                                ];
                                break;
                            case 'ni'://不包含
                                $params['body']['query']["bool"]["must_not"][]['query_string'] = [
                                    "gte"=>$v['data']
                                ];
                                break;
                        }

                        break;
                    case 'OR':
                        $params['body']['query']['bool']["minimum_should_match"] = 1;
                        switch ($v['op']){
                            case 'eq'://相等
                                $params['body']['query']['bool']['should'][]['bool']["must"][]["match"] = [$v['field']=>$v['data']];
                                break;
                            case 'ne'://不等
                                $params['body']['query']['bool']['should'][]['bool']["must_not"][]["match"] = [$v['field']=>$v['data']];
                                break;
                            case 'bw'://开始于
                                $params['body']['query']['bool']['should'][]['bool']["must"][]['range'][$v['field']] = [
                                    "gte"=>$v['data']
                                ];
                                break;
                            case 'bn'://不开始于
                                $params['body']['query']['bool']['should'][]['bool']["must"][]['range'][$v['field']] = [
                                    "lt"=>$v['data']
                                ];
                                break;
                            case 'ew'://结束于
                                $params['body']['query']['bool']['should'][]['bool']["must"][]['range'][$v['field']] = [
                                    "lte"=>$v['data']
                                ];
                                break;
                            case 'nn'://非空
                                $params['body']['query']['bool']['should'][]['bool']["must"][]["exists"] = [
                                    "field"=>$v['field']
                                ];
                                break;
                            case 'nu'://空值
                                $params['body']['query']['bool']['should'][]['bool']["must_not"][]["exists"] = [
                                    "field"=>$v['field']
                                ];
                                break;
                            case 'en'://不结束于
                                $params['body']['query']['bool']['should'][]['bool']["must"][]['range'][$v['field']] = [
                                    "gt"=>$v['data']
                                ];
                                break;
                            case 'cn'://包含
                                $params['body']['query']['bool']['should'][]['bool']["must"][]['query_string'] = [
                                    'default_field' => $v['field'],
                                    'query' => $v['data']
                                ];
                                break;
                            case 'nc'://不包含
                                $params['body']['query']['bool']['should'][]['bool']["must_not"][]['query_string'] = [
                                    "gte"=>$v['data']
                                ];
                                break;
                            case 'in'://包含
                                $params['body']['query']['bool']['should'][]['bool']["must"][]['query_string'] = [
                                    'default_field' => $v['field'],
                                    'query' => $v['data']
                                ];
                                break;
                            case 'ni'://不包含
                                $params['body']['query']['bool']['should'][]['bool']["must_not"][]['query_string'] = [
                                    "gte"=>$v['data']
                                ];
                                break;
                        }
                        break;
                }
            }

        }


        return  self::$esclient->search($params);

    }


    /**
     * 添加数据
     * @param array $body
     * @param string $id  索引
     * @return mixed
     */
    public function Add($body=[], $id=''){

        $params = [
            'index' => self::$index,
            'type' => self::$type,
            'body' => $body
        ];

        ($id != '') && ($params['id'] = $id);

        return self::$esclient->index($params);

    }

    /**
     * 删除数据
     * @param string $id
     * @return string
     */
    public function Delete($id=''){

        if ($id == '')
            return 'id is require';

        $params = [
            'index' => self::$index,
            'type' => self::$type,
            'id' => $id
        ];


        return self::$esclient->delete($params);
    }


}


function main(){

    /*$index='texas-30001-game';
    $type= 'game';
    $stime= "2018-05-01 00:00:00";
    $etime= "2018-05-04 00:00:00";

    $ins = \Pelasticsearch\Pelasticsearch::Getins();
    $ins->init($index,$type,$stime,$etime);
    $data = $ins->simpleQuery(0,1);

    echo '<pre>';
    print_r($data);
    echo '</pre>';*/


    /*$index='texas-30001-player';
    $type= 'player';
    $stime= "2018-05-01 00:00:00";
    $etime= "2018-05-08 00:00:00";
    $groupOp = 'AND';

    $querydata = [
        [
            "op"=>"eq",
            "field"=>"ActionName",
            "data"=>"player.gain"
        ],
        [
            "op"=>"eq",
            "field"=>"type",
            "data"=>"gold"
        ],
        [
            "op"=>"ew",
            "field"=>"num",
            "data"=>10000
        ]
    ];

    $ins = \Pelasticsearch\Pelasticsearch::Getins();
    $ins->init($index,$type,$stime,$etime);
    $data = $ins->multipleQuery(0,100,$groupOp,$querydata);

    echo '<pre>';
    print_r($data);
    echo '</pre>';*/


    /*$index='texas-30001-player';
    $type= 'player';
    $body = [
        "fieldname"=>"fielddata"
    ];
    $ins = \Pelasticsearch\Pelasticsearch::Getins();
    $ins->init($index,$type);
    $data = $ins->Add($body);


    echo '<pre>';
    print_r($data);
    echo '</pre>';*/

    /*$index='texas-30001-player';
    $type= 'my_type';
    $id = 'my_id';
    $ins = \Pelasticsearch\Pelasticsearch::Getins();
    $ins->init($index,$type);
    $data = $ins->Delete($id);

    echo '<pre>';
    print_r($data);
    echo '</pre>';*/



}

main();
