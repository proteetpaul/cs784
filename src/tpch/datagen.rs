use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

const BATCH_SIZE: usize = 65_536;

// ---------------------------------------------------------------------------
// Arrow conversion helpers
// ---------------------------------------------------------------------------

fn date_to_days(d: tpchgen::dates::TPCHDate) -> i32 {
    d.to_unix_epoch()
}

fn dec_to_f64(d: tpchgen::decimal::TPCHDecimal) -> f64 {
    d.as_f64()
}

// ---------------------------------------------------------------------------
// Per-table builders.  Each collects from a tpchgen iterator into
// Vec<RecordBatch> (one batch per BATCH_SIZE rows).
// ---------------------------------------------------------------------------

fn build_region(sf: f64) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, true),
    ]));
    let gen = RegionGenerator::new(sf, 1, 1);
    let mut keys = Vec::new();
    let mut names = StringBuilder::new();
    let mut comments = StringBuilder::new();
    for r in gen.iter() {
        keys.push(r.r_regionkey);
        names.append_value(r.r_name);
        comments.append_value(r.r_comment);
    }
    vec![RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(names.finish()),
        Arc::new(comments.finish()),
    ]).unwrap()]
}

fn build_nation(sf: f64) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, true),
    ]));
    let gen = NationGenerator::new(sf, 1, 1);
    let mut nk = Vec::new();
    let mut nm = StringBuilder::new();
    let mut rk = Vec::new();
    let mut cm = StringBuilder::new();
    for n in gen.iter() {
        nk.push(n.n_nationkey);
        nm.append_value(n.n_name);
        rk.push(n.n_regionkey);
        cm.append_value(n.n_comment);
    }
    vec![RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(nk)),
        Arc::new(nm.finish()),
        Arc::new(Int64Array::from(rk)),
        Arc::new(cm.finish()),
    ]).unwrap()]
}

fn build_supplier(sf: f64) -> Vec<RecordBatch> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, true),
    ]));
    let gen = SupplierGenerator::new(sf, 1, 1);
    let mut batches = Vec::new();
    let mut sk = Vec::with_capacity(BATCH_SIZE);
    let mut sn = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20);
    let mut sa = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 30);
    let mut nk = Vec::with_capacity(BATCH_SIZE);
    let mut ph = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16);
    let mut ab = Vec::with_capacity(BATCH_SIZE);
    let mut cm = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 40);

    let mut flush = |sk: &mut Vec<i64>, sn: &mut StringBuilder, sa: &mut StringBuilder,
                     nk: &mut Vec<i64>, ph: &mut StringBuilder, ab: &mut Vec<f64>,
                     cm: &mut StringBuilder, schema: &SchemaRef| {
        if sk.is_empty() { return; }
        batches.push(RecordBatch::try_new(Arc::clone(schema), vec![
            Arc::new(Int64Array::from(std::mem::take(sk))),
            Arc::new(sn.finish()),
            Arc::new(sa.finish()),
            Arc::new(Int64Array::from(std::mem::take(nk))),
            Arc::new(ph.finish()),
            Arc::new(Float64Array::from(std::mem::take(ab))),
            Arc::new(cm.finish()),
        ]).unwrap());
    };

    for s in gen.iter() {
        sk.push(s.s_suppkey);
        sn.append_value(s.s_name.to_string());
        sa.append_value(s.s_address.to_string());
        nk.push(s.s_nationkey);
        ph.append_value(s.s_phone.to_string());
        ab.push(dec_to_f64(s.s_acctbal));
        cm.append_value(&s.s_comment);
        if sk.len() >= BATCH_SIZE {
            flush(&mut sk, &mut sn, &mut sa, &mut nk, &mut ph, &mut ab, &mut cm, &schema);
        }
    }
    flush(&mut sk, &mut sn, &mut sa, &mut nk, &mut ph, &mut ab, &mut cm, &schema);
    batches
}

fn build_customer(sf: f64) -> Vec<RecordBatch> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, true),
    ]));
    let gen = CustomerGenerator::new(sf, 1, 1);
    let mut batches = Vec::new();
    let (mut ck, mut cn, mut ca, mut nk, mut ph, mut ab, mut ms, mut cm) = (
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 30),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 40),
    );

    let mut flush = |ck: &mut Vec<i64>, cn: &mut StringBuilder, ca: &mut StringBuilder,
                     nk: &mut Vec<i64>, ph: &mut StringBuilder, ab: &mut Vec<f64>,
                     ms: &mut StringBuilder, cm: &mut StringBuilder, schema: &SchemaRef| {
        if ck.is_empty() { return; }
        batches.push(RecordBatch::try_new(Arc::clone(schema), vec![
            Arc::new(Int64Array::from(std::mem::take(ck))),
            Arc::new(cn.finish()),
            Arc::new(ca.finish()),
            Arc::new(Int64Array::from(std::mem::take(nk))),
            Arc::new(ph.finish()),
            Arc::new(Float64Array::from(std::mem::take(ab))),
            Arc::new(ms.finish()),
            Arc::new(cm.finish()),
        ]).unwrap());
    };

    for c in gen.iter() {
        ck.push(c.c_custkey);
        cn.append_value(c.c_name.to_string());
        ca.append_value(c.c_address.to_string());
        nk.push(c.c_nationkey);
        ph.append_value(c.c_phone.to_string());
        ab.push(dec_to_f64(c.c_acctbal));
        ms.append_value(c.c_mktsegment);
        cm.append_value(c.c_comment);
        if ck.len() >= BATCH_SIZE {
            flush(&mut ck, &mut cn, &mut ca, &mut nk, &mut ph, &mut ab, &mut ms, &mut cm, &schema);
        }
    }
    flush(&mut ck, &mut cn, &mut ca, &mut nk, &mut ph, &mut ab, &mut ms, &mut cm, &schema);
    batches
}

fn build_part(sf: f64) -> Vec<RecordBatch> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, true),
    ]));
    let gen = PartGenerator::new(sf, 1, 1);
    let mut batches = Vec::new();
    let (mut pk, mut pn, mut mf, mut br, mut tp, mut sz, mut ct, mut rp, mut cm) = (
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 40),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 28),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20),
    );

    let mut flush = |pk: &mut Vec<i64>, pn: &mut StringBuilder, mf: &mut StringBuilder,
                     br: &mut StringBuilder, tp: &mut StringBuilder, sz: &mut Vec<i32>,
                     ct: &mut StringBuilder, rp: &mut Vec<f64>, cm: &mut StringBuilder,
                     schema: &SchemaRef| {
        if pk.is_empty() { return; }
        batches.push(RecordBatch::try_new(Arc::clone(schema), vec![
            Arc::new(Int64Array::from(std::mem::take(pk))),
            Arc::new(pn.finish()),
            Arc::new(mf.finish()),
            Arc::new(br.finish()),
            Arc::new(tp.finish()),
            Arc::new(Int32Array::from(std::mem::take(sz))),
            Arc::new(ct.finish()),
            Arc::new(Float64Array::from(std::mem::take(rp))),
            Arc::new(cm.finish()),
        ]).unwrap());
    };

    for p in gen.iter() {
        pk.push(p.p_partkey);
        pn.append_value(p.p_name.to_string());
        mf.append_value(p.p_mfgr.to_string());
        br.append_value(p.p_brand.to_string());
        tp.append_value(p.p_type);
        sz.push(p.p_size);
        ct.append_value(p.p_container);
        rp.push(dec_to_f64(p.p_retailprice));
        cm.append_value(p.p_comment);
        if pk.len() >= BATCH_SIZE {
            flush(&mut pk, &mut pn, &mut mf, &mut br, &mut tp, &mut sz, &mut ct, &mut rp, &mut cm, &schema);
        }
    }
    flush(&mut pk, &mut pn, &mut mf, &mut br, &mut tp, &mut sz, &mut ct, &mut rp, &mut cm, &schema);
    batches
}

fn build_partsupp(sf: f64) -> Vec<RecordBatch> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, true),
    ]));
    let gen = PartSuppGenerator::new(sf, 1, 1);
    let mut batches = Vec::new();
    let (mut pk, mut sk, mut aq, mut sc, mut cm) = (
        Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 40),
    );

    let mut flush = |pk: &mut Vec<i64>, sk: &mut Vec<i64>, aq: &mut Vec<i32>,
                     sc: &mut Vec<f64>, cm: &mut StringBuilder, schema: &SchemaRef| {
        if pk.is_empty() { return; }
        batches.push(RecordBatch::try_new(Arc::clone(schema), vec![
            Arc::new(Int64Array::from(std::mem::take(pk))),
            Arc::new(Int64Array::from(std::mem::take(sk))),
            Arc::new(Int32Array::from(std::mem::take(aq))),
            Arc::new(Float64Array::from(std::mem::take(sc))),
            Arc::new(cm.finish()),
        ]).unwrap());
    };

    for ps in gen.iter() {
        pk.push(ps.ps_partkey);
        sk.push(ps.ps_suppkey);
        aq.push(ps.ps_availqty);
        sc.push(dec_to_f64(ps.ps_supplycost));
        cm.append_value(ps.ps_comment);
        if pk.len() >= BATCH_SIZE {
            flush(&mut pk, &mut sk, &mut aq, &mut sc, &mut cm, &schema);
        }
    }
    flush(&mut pk, &mut sk, &mut aq, &mut sc, &mut cm, &schema);
    batches
}

fn build_orders(sf: f64) -> Vec<RecordBatch> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, true),
    ]));
    let gen = OrderGenerator::new(sf, 1, 1);
    let mut batches = Vec::new();
    let (mut ok, mut ck, mut os, mut tp, mut od, mut op, mut cl, mut sp, mut cm) = (
        Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 40),
    );

    let mut flush = |ok: &mut Vec<i64>, ck: &mut Vec<i64>, os: &mut StringBuilder,
                     tp: &mut Vec<f64>, od: &mut Vec<i32>, op: &mut StringBuilder,
                     cl: &mut StringBuilder, sp: &mut Vec<i32>, cm: &mut StringBuilder,
                     schema: &SchemaRef| {
        if ok.is_empty() { return; }
        batches.push(RecordBatch::try_new(Arc::clone(schema), vec![
            Arc::new(Int64Array::from(std::mem::take(ok))),
            Arc::new(Int64Array::from(std::mem::take(ck))),
            Arc::new(os.finish()),
            Arc::new(Float64Array::from(std::mem::take(tp))),
            Arc::new(Date32Array::from(std::mem::take(od))),
            Arc::new(op.finish()),
            Arc::new(cl.finish()),
            Arc::new(Int32Array::from(std::mem::take(sp))),
            Arc::new(cm.finish()),
        ]).unwrap());
    };

    for o in gen.iter() {
        ok.push(o.o_orderkey);
        ck.push(o.o_custkey);
        os.append_value(o.o_orderstatus.to_string());
        tp.push(dec_to_f64(o.o_totalprice));
        od.push(date_to_days(o.o_orderdate));
        op.append_value(o.o_orderpriority);
        cl.append_value(o.o_clerk.to_string());
        sp.push(o.o_shippriority);
        cm.append_value(o.o_comment);
        if ok.len() >= BATCH_SIZE {
            flush(&mut ok, &mut ck, &mut os, &mut tp, &mut od, &mut op, &mut cl, &mut sp, &mut cm, &schema);
        }
    }
    flush(&mut ok, &mut ck, &mut os, &mut tp, &mut od, &mut op, &mut cl, &mut sp, &mut cm, &schema);
    batches
}

fn build_lineitem(sf: f64) -> Vec<RecordBatch> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, true),
    ]));
    let gen = LineItemGenerator::new(sf, 1, 1);
    let mut batches = Vec::new();
    let (mut ok, mut pk, mut sk, mut ln, mut qt, mut ep, mut dc, mut tx,
         mut rf, mut ls, mut sd, mut cd, mut rd, mut si, mut sm, mut cm) = (
        Vec::with_capacity(BATCH_SIZE), Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE), Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE), Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE), Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE), Vec::with_capacity(BATCH_SIZE),
        Vec::with_capacity(BATCH_SIZE),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 8),
        StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 30),
    );

    let mut flush = |ok: &mut Vec<i64>, pk: &mut Vec<i64>, sk: &mut Vec<i64>,
                     ln: &mut Vec<i32>, qt: &mut Vec<f64>, ep: &mut Vec<f64>,
                     dc: &mut Vec<f64>, tx: &mut Vec<f64>,
                     rf: &mut StringBuilder, ls: &mut StringBuilder,
                     sd: &mut Vec<i32>, cd: &mut Vec<i32>, rd: &mut Vec<i32>,
                     si: &mut StringBuilder, sm: &mut StringBuilder, cm: &mut StringBuilder,
                     schema: &SchemaRef| {
        if ok.is_empty() { return; }
        batches.push(RecordBatch::try_new(Arc::clone(schema), vec![
            Arc::new(Int64Array::from(std::mem::take(ok))),
            Arc::new(Int64Array::from(std::mem::take(pk))),
            Arc::new(Int64Array::from(std::mem::take(sk))),
            Arc::new(Int32Array::from(std::mem::take(ln))),
            Arc::new(Float64Array::from(std::mem::take(qt))),
            Arc::new(Float64Array::from(std::mem::take(ep))),
            Arc::new(Float64Array::from(std::mem::take(dc))),
            Arc::new(Float64Array::from(std::mem::take(tx))),
            Arc::new(rf.finish()),
            Arc::new(ls.finish()),
            Arc::new(Date32Array::from(std::mem::take(sd))),
            Arc::new(Date32Array::from(std::mem::take(cd))),
            Arc::new(Date32Array::from(std::mem::take(rd))),
            Arc::new(si.finish()),
            Arc::new(sm.finish()),
            Arc::new(cm.finish()),
        ]).unwrap());
    };

    for l in gen.iter() {
        ok.push(l.l_orderkey);
        pk.push(l.l_partkey);
        sk.push(l.l_suppkey);
        ln.push(l.l_linenumber);
        qt.push(l.l_quantity as f64);
        ep.push(dec_to_f64(l.l_extendedprice));
        dc.push(dec_to_f64(l.l_discount));
        tx.push(dec_to_f64(l.l_tax));
        rf.append_value(l.l_returnflag);
        ls.append_value(l.l_linestatus);
        sd.push(date_to_days(l.l_shipdate));
        cd.push(date_to_days(l.l_commitdate));
        rd.push(date_to_days(l.l_receiptdate));
        si.append_value(l.l_shipinstruct);
        sm.append_value(l.l_shipmode);
        cm.append_value(l.l_comment);
        if ok.len() >= BATCH_SIZE {
            flush(&mut ok, &mut pk, &mut sk, &mut ln, &mut qt, &mut ep,
                  &mut dc, &mut tx, &mut rf, &mut ls, &mut sd, &mut cd,
                  &mut rd, &mut si, &mut sm, &mut cm, &schema);
        }
    }
    flush(&mut ok, &mut pk, &mut sk, &mut ln, &mut qt, &mut ep,
          &mut dc, &mut tx, &mut rf, &mut ls, &mut sd, &mut cd,
          &mut rd, &mut si, &mut sm, &mut cm, &schema);
    batches
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

fn register(ctx: &SessionContext, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
    let schema: SchemaRef = batches[0].schema();
    ctx.register_table(name, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    Ok(())
}

/// Generate spec-compliant TPC-H data (via `tpchgen`) and register all
/// eight tables as in-memory tables on `ctx`.
pub async fn register_tpch_tables(ctx: &SessionContext, sf: f64) -> Result<()> {
    log::info!("Generating TPC-H data at scale factor {sf} ...");

    register(ctx, "region", build_region(sf))?;
    register(ctx, "nation", build_nation(sf))?;
    register(ctx, "supplier", build_supplier(sf))?;
    register(ctx, "customer", build_customer(sf))?;
    register(ctx, "part", build_part(sf))?;
    register(ctx, "partsupp", build_partsupp(sf))?;
    register(ctx, "orders", build_orders(sf))?;
    register(ctx, "lineitem", build_lineitem(sf))?;

    log::info!("All TPC-H tables registered.");
    Ok(())
}
