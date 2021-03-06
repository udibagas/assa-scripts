<?php
require_once(dirname(__FILE__).'/query.php');

class XmlToDb{
	private $db_host 		= '192.168.1.235';
	private $db_uname 		= 'lamsolusi';
	private $db_pass 		= '4rfv!@#$';
	private $db_name 		= 'assarent_lamjaya';
	private $file_source		= '/../file/input/';
	private $qbuilder;
	protected $conn = false;

	public function __construct(){
		$this->qbuilder	= new Myquery();
	}

	private function connect()
	{
		$conn = mysqli_connect($this->db_host, $this->db_uname, $this->db_pass, $this->db_name);

		if (mysqli_connect_errno()) {
		    printf("Connect failed: %s\n", mysqli_connect_error());
		    exit();
		}

		$this->conn = $conn;
	}

	private function connect_close($con){
		mysqli_close($con);
	}

	public function parse_to_tbl($table, $file_name,$fild_contract_header){
		$this->cekDir();

	    $is_faild   = false;
	    $dom        = new DOMDocument();
	    $dir_func   = dirname(__FILE__).$this->file_source;
	    $cdom       = $dom->loadXML(file_get_contents($dir_func.$file_name));
	    $data       = $dom->getElementsByTagName('string');

	    if($cdom){
	        foreach ($data as $row){
	            $datas      = $row->nodeValue;
	            $array      = (explode('|', $datas));
	            $koma       = str_replace("'","\'",$array);
	            $json       = json_encode($koma);
	            $ins        = json_decode($json, true);
	            $colomns    = "`".implode("`, `", explode(",", $fild_contract_header))."`";
	            $es_value   = array_values($ins);
	            $values     = "'".implode("', '", $es_value)."'";

	            if($table=='TF_CLEARING_RESET'){
	            	$values .=", '".date('Y-m-d H:i:s')."'";
	            	$clearing_no = trim($es_value[0]);
	            	$this->reset_clearing($clearing_no);
	            }

	            if($table=='MF_TIME_PERIOD'){
	            	$ar_tocek 		= explode(',', $values);
	            	$bln_one_from	= str_replace("'", "", $ar_tocek[2]);
	            	$bln_one_to		= str_replace("'", "", $ar_tocek[4]);
	            	$bln_two_from	= str_replace("'", "", $ar_tocek[6]);
	            	$bln_two_to		= str_replace("'", "", $ar_tocek[8]);

	            	foreach ($ar_tocek as $key => $value) {

						$ar_tocek[2] = (($bln_one_from+0) > 12) ? "'12'":"'".substr($bln_one_from, 2,2)."'";
						$ar_tocek[4] = (($bln_one_to+0) > 12) ? "'12'":"'".substr($bln_one_to, 2,2)."'";
						$ar_tocek[6] = (($bln_two_from+0) > 12) ? "'12'":"'".substr($bln_two_from, 2,2)."'";
						$ar_tocek[8] = (($bln_two_to+0) > 12) ? "'12'":"'".substr($bln_two_to, 2,2)."qq'";
					}
	            	$values     = implode(", ", $ar_tocek);
	            }

	           	if($table=='MF_CONTRACT_BILLING_PLAN'){
	           		$ar_tocek 	= explode(',', $values);
					$from 		= str_replace("'", "", $ar_tocek[5]);
					$to 		= str_replace("'", "", $ar_tocek[4]);

					if($from > $to){
						foreach ($ar_tocek as $key => $value) {
							$ar_tocek[4] = "'".$from."'";
							$ar_tocek[5] = "'".$to."'";
						}
						$values     = implode(", ", $ar_tocek);
					}
	           	}

	            $query      = "REPLACE INTO ".$table." ($colomns) VALUES ($values)";

	            $mcon = $this->connect();
	            $sql= mysqli_query($mcon, $query);
	            if (! $sql) {
	                 $is_faild = true;
	            }
	            $this->connect_close($mcon);
	        }
	    }else
	        $is_faild   = true;

	    if($is_faild){
	        if (!is_dir($dir_func.'failed/'))
	            mkdir($dir_func.'failed/', 0777, true);

	        if(!is_file($dir_func."failed/index.html")){
	            $content = "<html><head><title>403 Forbidden</title></head><body><p>Directory access is forbidden.</p></body></html>";
	            file_put_contents($dir_func."failed/index.html", $content);
	        }

	        rename($dir_func.$file_name, $dir_func.'failed/'.$file_name);
	    }else{
	        if(!is_dir($dir_func.'success/'))
	            mkdir($dir_func.'success/', 0777, true);

	        if(!is_file($dir_func."success/index.html")){
	            $content = "<html><head><title>403 Forbidden</title></head><body><p>Directory access is forbidden.</p></body></html>";
	            file_put_contents($dir_func."success/index.html", $content);
	        }

	        rename($dir_func.$file_name, $dir_func.'success/old_'.$file_name);
    	}
    }

    private function cekDir(){
		$dir = dirname(__FILE__).$this->file_source;
		if (!is_dir($dir)) {
		    mkdir($dir, 0777, true);
		}
	}


	private function reset_clearing($clearing_no){
		$doc_reset  = array();
		$initial_ar = array('ARL','ARI');
		$initial_bp = array('BPL','BPI','ARB');
		$initial_sp = array('SPL','SPI');

		$ar_find_doc = array(
			'select'=>'CONCAT(CHR_BUKRS,VCH_REBZG,DYR_REBZJ,INT_REBZZ) AS doc_to_reset',
			'from'=> 'TF_BSEG',
			'where'=>array(
				'VCH_BELNR'=> $clearing_no
			)
		);

		$q_find_doc = $this->qbuilder->build_query($ar_find_doc);

		$res_find_doc= mysqli_query($this->connect(), $q_find_doc);
		if(mysqli_num_rows($res_find_doc)>0){
				while ($row_find_doc = mysqli_fetch_array($res_find_doc)){
				$doc_reset[] = $row_find_doc['doc_to_reset'];
			}
		}

		if(count($doc_reset) > 0){
			$q_reset  = "UPDATE `TF_BSEG` SET `DAT_AUGCP` =NULL, `VCH_AUGBL` =NULL ";

			if(in_array(substr($clearing_no, 0,3), $initial_ar))
				$q_reset  .=", `ar_status`='0' ";

			if(in_array(substr($clearing_no, 0,3), $initial_bp))
				$q_reset  .=", `bp_status`='0' ";

			if(in_array(substr($clearing_no, 0,3), $initial_sp))
				$q_reset  .=", `ssp_status`='0' ";


			$q_reset .= "WHERE CONCAT(CHR_BUKRS,VCH_BELNR,DYR_GJAHR,VCH_BUZEI) IN('".implode("','", $doc_reset)."')";

			$res_reset= mysqli_query($this->connect(), $q_reset);
			if($res_reset){
				$q_reset_m = mysqli_query($this->connect(),'UPDATE `TF_BSEG` SET VCH_REBZG = NULL, DYR_REBZJ=NULL,INT_REBZZ=NULL WHERE VCH_BELNR="'.$clearing_no.'"');
			}
			unset($doc_reset);

		}

	}
}
