package main

import (
	"testing"
)

func TestParseResponse(t *testing.T) {
	body := `
	{"meta":[{"name":"t","type":"UInt64"},{"name":"unitId_cc","type":"Array(Tuple(Tuple(Int64, String), Float64))"}],"data":[{"t":"1567080720000","unitId_cc":[[["80841","IN"],null],[["57603","CN"],0],[["31247","CN"],0],[["112391","IN"],1],[["80841","ID"],1],[["10856","CN"],0.02702702702702703],[["57601","CN"],0]]},{"t":"1567080730000","unitId_cc":[[["20680","IN"],0.9016064257028112],[["10856","CN"],0.00558659217877095],[["80841","ID"],0.7805814591332968],[["112391","IN"],0.7262638717632552],[["80841","IN"],0.7147706120661338],[["57603","CN"],0.16326530612244897],[["80841","RU"],0.6923076923076923],[["57601","CN"],0.010752688172043012],[["31247","CN"],0.014634146341463415]]},{"t":"1567080740000","unitId_cc":[[["87261","IN"],0.43567052416609936],[["57601","CN"],0.0196078431372549],[["80841","RU"],0.5779005524861879],[["80841","ID"],1],[["31247","CN"],0.020833333333333332],[["10856","CN"],0.021321961620469083],[["112391","IN"],0.6],[["57603","CN"],0.13680781758957655]]},{"t":"1567080750000","unitId_cc":[[["31247","CN"],0],[["10856","CN"],0.010676156583629894],[["57603","CN"],0.03333333333333333],[["57601","CN"],0.016666666666666666],[["87261","IN"],0.41963214714114355],[["80841","IN"],0.6085090812321203],[["20680","IN"],0.9333333333333333]]},{"t":"1567080760000","unitId_cc":[[["80841","ID"],0.6441],[["31247","CN"],0.015066336856307623],[["57601","CN"],0.00826739104759655],[["57603","CN"],0.11229521234292986],[["112391","IN"],0.75],[["80841","IN"],0.5164168565651378],[["87261","IN"],0.4280542431122292],[["20680","IN"],null],[["80841","RU"],0.5605955334987593],[["10856","CN"],0.0028077434609132556]]},{"t":"1567080770000","unitId_cc":[[["80841","IN"],0.47122682554668416],[["112391","IN"],0.7163375640995893],[["57603","CN"],0.09999400335811945],[["31247","CN"],0.012756105019439955],[["10856","CN"],0.0016205504315294293],[["87261","IN"],0.4285746090053988],[["57601","CN"],0.01209807258843553],[["20680","IN"],0.8197183650286923],[["80841","ID"],0.5529932251609325],[["80841","RU"],0.5652224416308514]]},{"t":"1567080780000","unitId_cc":[[["87261","IN"],0],[["57601","CN"],0],[["10856","CN"],0.14705882352941177],[["57603","CN"],0.16666666666666666],[["80841","RU"],0.8571428571428571],[["31247","CN"],0]]},{"t":"1567080790000","unitId_cc":[[["112391","IN"],0],[["57601","CN"],0],[["10856","CN"],0.003424657534246575],[["80841","IN"],1],[["57603","CN"],0.10505836575875487],[["31247","CN"],0.013888888888888888],[["87261","IN"],0.4376653147593017]]},{"t":"1567080800000","unitId_cc":[[["20680","IN"],0.9049676025917927],[["80841","IN"],0.6360096025606828],[["57601","CN"],0.016296296296296295],[["112391","IN"],0.7289603960396039],[["80841","RU"],0.5435684647302904],[["31247","CN"],0.013100436681222707],[["80841","ID"],0.7236533957845434],[["10856","CN"],0.005376344086021506],[["57603","CN"],0],[["87261","IN"],0.4297]]},{"t":"1567080810000","unitId_cc":[[["112391","IN"],0],[["10856","CN"],0.029490616621983913],[["57603","CN"],0],[["57601","CN"],0],[["80841","IN"],0.5695430456954305],[["31247","CN"],0],[["87261","IN"],0.4214]]},{"t":"1567080820000","unitId_cc":[[["10856","CN"],0.0021591158603527774],[["57603","CN"],0.10592011412268189],[["80841","RU"],0.5600027843519421],[["112391","IN"],0.3333333333333333],[["31247","CN"],0.015044704264099037],[["80841","IN"],0.5186701277955271],[["57601","CN"],0.01242124212421242],[["80841","ID"],0.6199]]},{"t":"1567080830000","unitId_cc":[[["57603","CN"],0.09695086305036768],[["80841","IN"],0.4633890019591806],[["31247","CN"],0.013446834992887624],[["80841","ID"],0.5499839011650787],[["112391","IN"],0.7152904991292942],[["87261","IN"],0.4261828743074827],[["57601","CN"],0.01157613535173642],[["10856","CN"],0.0015164060246904392],[["80841","RU"],0.5615964934462848],[["20680","IN"],0.8132840895767381]]},{"t":"1567080840000","unitId_cc":[[["80841","IN"],null],[["57601","CN"],0],[["57603","CN"],0],[["10856","CN"],0.025],[["31247","CN"],0]]},{"t":"1567080850000","unitId_cc":[[["10856","CN"],0.005988023952095809],[["80841","IN"],0.42857142857142855],[["80841","RU"],0.5725190839694656],[["57601","CN"],0.013215859030837005],[["57603","CN"],0.13285457809694792],[["31247","CN"],0.014598540145985401]]},{"t":"1567080860000","unitId_cc":[[["57601","CN"],0],[["87261","IN"],0.42844928099494756],[["31247","CN"],0],[["20680","IN"],0.9029535864978903],[["80841","ID"],0.76],[["57603","CN"],0],[["10856","CN"],0.07936507936507936],[["112391","IN"],0.7007952286282306],[["80841","IN"],0.7122928564741129]]},{"t":"1567080870000","unitId_cc":[[["87261","IN"],0.437125748502994],[["31247","CN"],0.0026109660574412533],[["57601","CN"],0.012987012987012988],[["57603","CN"],0.046511627906976744],[["10856","CN"],0.00691699604743083],[["80841","ID"],1],[["80841","IN"],0.6344487401679776]]},{"t":"1567080880000","unitId_cc":[[["57603","CN"],0.1005868544600939],[["31247","CN"],0.013103870009609504],[["87261","IN"],0.43395],[["57601","CN"],0.011813628201493242],[["80841","RU"],0.7721518987341772],[["10856","CN"],0.00223872243573001],[["80841","ID"],0.6741],[["80841","IN"],0.5485666666666666]]},{"t":"1567080890000","unitId_cc":[[["20680","IN"],0.8203493862134089],[["31247","CN"],0.014367237327962403],[["57603","CN"],0.10278279275353297],[["57601","CN"],0.01133574850999182],[["80841","IN"],0.4797960683581208],[["10856","CN"],0.0016105622922421461],[["112391","IN"],0.7148707448169951],[["87261","IN"],0.4247594050743657],[["80841","ID"],0.5543757431629013],[["80841","RU"],0.5617313915857605]]},{"t":"1567080900000","unitId_cc":[[["80841","IN"],0],[["10856","CN"],0.17391304347826086],[["31247","CN"],0],[["57601","CN"],0],[["57603","CN"],0]]},{"t":"1567080910000","unitId_cc":[[["80841","IN"],1],[["112391","IN"],0.5],[["57601","CN"],0.015267175572519083],[["80841","RU"],0.8181818181818182],[["10856","CN"],0.008708272859216255],[["57603","CN"],0.08754208754208755],[["31247","CN"],0.004189944134078212]]},{"t":"1567080920000","unitId_cc":[[["87261","IN"],0.4037325773682967],[["57601","CN"],0],[["80841","RU"],0.5764966740576497],[["112391","IN"],0.6986128625472888],[["80841","IN"],0.6002805805859818],[["20680","IN"],0.8614318706697459],[["57603","CN"],0.06639004149377593],[["31247","CN"],0],[["10856","CN"],0.002012072434607646],[["80841","ID"],0.6811320754716981]]},{"t":"1567080930000","unitId_cc":[[["80841","ID"],1],[["31247","CN"],0],[["57603","CN"],0.06593406593406594],[["80841","IN"],0.5687],[["57601","CN"],0.0014265335235378032],[["10856","CN"],0.05319148936170213]]},{"t":"1567080940000","unitId_cc":[[["80841","ID"],0.6055],[["80841","RU"],0.5603970223325062],[["10856","CN"],0.0022879917337072847],[["112391","IN"],0],[["80841","IN"],0.51455],[["57603","CN"],0.11049723756906077],[["31247","CN"],0.00629156929714183],[["87261","IN"],0.38195],[["57601","CN"],0.012874473879673186]]},{"t":"1567080950000","unitId_cc":[[["80841","IN"],0.4584429436526654],[["57603","CN"],0.062563169597736],[["10856","CN"],0.0016289056715533836],[["80841","ID"],0.5411716122942599],[["112391","IN"],0.7094097903420238],[["31247","CN"],0.007092771839068846],[["80841","RU"],0.5579210298779902],[["20680","IN"],0.8140229430379747],[["87261","IN"],0.35253420908593325],[["57601","CN"],0.01251488529894174]]},{"t":"1567080960000","unitId_cc":[[["57603","CN"],0.04411764705882353],[["80841","IN"],null],[["10856","CN"],0.16326530612244897],[["57601","CN"],0],[["31247","CN"],0]]},{"t":"1567080970000","unitId_cc":[[["87261","IN"],0.3591068462816018],[["57601","CN"],0.006633499170812604],[["57603","CN"],0.06656804733727811],[["80841","ID"],1],[["10856","CN"],0.008068854222700376],[["31247","CN"],0.012681159420289856],[["80841","RU"],0.8]]},{"t":"1567080980000","unitId_cc":[[["80841","RU"],0.5607235142118863],[["112391","IN"],0.6666666666666666],[["57603","CN"],0],[["80841","IN"],0.6905741370059023],[["80841","ID"],0.7292225201072386],[["20680","IN"],0.887987012987013],[["57601","CN"],0.02318840579710145],[["31247","CN"],0],[["10856","CN"],0.006993006993006993]]},{"t":"1567080990000","unitId_cc":[[["31247","CN"],0],[["10856","CN"],0.07407407407407407],[["20680","IN"],1],[["57603","CN"],0.024390243902439025],[["112391","IN"],0],[["57601","CN"],0.017391304347826087],[["87261","IN"],0],[["80841","IN"],0.5825666666666667]]},{"t":"1567081000000","unitId_cc":[[["80841","IN"],0.5307734613269337],[["80841","RU"],0.5613795458425815],[["31247","CN"],0.0005851375073142189],[["112391","IN"],0.705607476635514],[["10856","CN"],0.0033466281575207446],[["80841","ID"],0.6328],[["57603","CN"],0.040390879478827364],[["87261","IN"],0.3637571409592135],[["57601","CN"],0.0016467682173734047]]},{"t":"1567081010000","unitId_cc":[[["112391","IN"],0.7103342515548235],[["80841","IN"],0.4494795828721466],[["80841","ID"],0.5331413804090014],[["57601","CN"],0.011461318051575931],[["10856","CN"],0.0015779708335500876],[["87261","IN"],0.36535498895324536],[["31247","CN"],0.006108679795769512],[["80841","RU"],0.5585781860291664],[["20680","IN"],0.8092482158568775],[["57603","CN"],0.05831571090931708]]},{"t":"1567081020000","unitId_cc":[[["57603","CN"],null],[["57601","CN"],0],[["80841","RU"],null],[["10856","CN"],0],[["31247","CN"],0]]}],"rows":31,"rows_before_limit_at_least":31,"statistics":{"elapsed":0.247220728,"rows_read":8574156,"bytes_read":273636011}}
	`
	res, err := ParseResponse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Series) != 10 {
		t.Fatal("series length should be 10")
	}
}
