use test; # 改成实际使用的数据库
# 数据必须放在data表中，只支持int型表示的离散型数据（不为0）
# 需要预测的表放在predict表中，参数部分与data表相同，预留一个列放结果，并传入要放结果的列的名称
# predict表必须有一列为id（不可重复），作为每一行的唯一标识
drop function if exists calcEntropy;
drop function if exists calcAccur;
drop procedure if exists calcStatis;
drop procedure if exists nodeSplit;
drop procedure if exists buildDT;
drop procedure if exists goNextLevel;
drop procedure if exists predictDT;

delimiter $$
create function calcEntropy(
    _node_id int,
    _column_name varchar(20)
) returns double
begin

set @ins_count = (select sum(count) from node2statis where node_id=_node_id and column_name=_column_name group by node_id);
set @entro = ( select sum(-cc/@ins_count*pro*log(2,pro)) from
    (select tcc.column_value as cv,count/cc as pro, tcc.cc as cc
        from (select column_value,sum(count) as cc
            from node2statis where node_id=_node_id and column_name=_column_name
            group by column_value) as tcc 
        inner join (select column_value, count from node2statis
            where node_id=_node_id and column_name=_column_name) as n2s
        on n2s.column_value=tcc.column_value) as tp
    );

return @entro;

end $$

create function calcAccur(
    _node_id int,
    _column_name varchar(20)
) returns double
begin

set @ins_count = (select sum(count) from node2statis where node_id=_node_id and column_name=_column_name);
set @corr_count = (select sum(mc) from
    (select max(count) as mc from node2statis 
    where node_id=_node_id and column_name=_column_name 
    group by node_id,column_value) as cc);
return @corr_count/@ins_count;

end $$

create procedure calcStatis(
    _level int,
    class_name varchar(20)
)
begin
declare t_id int;
declare t_cn varchar(20);
declare flag int default 0;
declare cur cursor for 
    select id, column_name 
    from (select id from node where level=_level) as n
    inner join remain_column as rc on rc.node_id=n.id;
declare continue handler for not found set flag = 1;
open cur;
fetch cur into t_id, t_cn;
while flag!=1 do
    set @sql=concat('insert into node2statis(node_id, column_name, column_value, class, count) select '
        ,t_id,',"',t_cn,'",',t_cn,',',class_name,', sum(count) from (select ',t_cn,',',class_name
        ,',count from node2partition where node_id=',t_id,') as n2p group by '
        ,t_cn,',',class_name,';');
    prepare st from @sql;
    execute st;

    fetch cur into t_id, t_cn;
end while;
close cur;

drop view if exists tmpSta;
create view tmpSta as
    select node_id, class, sum(count) as count
    from node2statis as n2s
    where n2s.column_name=(
        select column_name
        from node2statis
        where node_id=n2s.node_id
        limit 1)    
    group by node_id, class;
insert into node2accur(node_id, ins_count)
    select node_id, sum(count)
    from tmpSta
    group by node_id;
update node2accur as n2a
    inner join(
        select node_id, max(count) as mc
        from tmpSta
        group by node_id
    ) as ts on n2a.node_id=ts.node_id
    set accur=ts.mc/ins_count,max_count=ts.mc;
update node
    inner join(
        select node_id, class
        from tmpSta
        where count>=all(
            select max_count from node2accur
            where node_id=tmpSta.node_id
        )
    ) as mc on id=node_id
    set node.class=mc.class;
update node2accur as n2a
    inner join(
        select node_id as node_id, sum(-pro*log(2,pro)) as en
        from (
            select n2a1.node_id as node_id, count/ins_count as pro
            from node2accur as n2a1 inner join tmpSta
            on n2a1.node_id = tmpSta.node_id
        ) as nt2 group by node_id
    ) as te on n2a.node_id=te.node_id
    set entro=te.en;

end $$

create procedure nodeSplit(
    _level int
)
begin

declare t_id int;
declare t_cn varchar(20);
declare flag int default 0;
declare cur cursor for select id, column_name from node where level = _level and column_name!='';
declare continue handler for not found set flag = 1;
# 获取该层的非叶子节点
truncate table lvNode;
insert into lvNode(id,column_name)
    select id, column_name from node where level=_level and column_name!='';
# 删除叶子节点的划分项
delete from node2partition where node_id in (
    select id from node where level=_level and column_name=''
);
# 插入新的边
insert into edge(from_id, column_value)
    select distinct id, column_value 
    from lvNode inner join node2statis as n2s 
    on lvNode.id=n2s.node_id and lvNode.column_name=n2s.column_name;
# 插入新的节点
insert into node(id, level)
    select to_id, _level+1
    from edge inner join lvNode on edge.from_id=lvNode.id;
# 更新新加节点的剩余行
insert into remain_column(node_id, column_name)
    select to_id, rc.column_name
    from edge inner join lvNode on from_id=id
    inner join remain_column as rc on node_id=id
    where rc.column_name!=lvNode.column_name;
open cur;
fetch cur into t_id, t_cn;
while flag != 1 do
    # 更新划分
    set @sql = concat('update node2partition as n2p inner join(select * from edge where from_id='
        ,t_id,') as e on n2p.node_id=e.from_id and e.column_value=n2p.',t_cn
        ,' set n2p.node_id=e.to_id where n2p.node_id=',t_id);
    prepare st from @sql;
    execute st;
    fetch cur into t_id, t_cn;
end while;
close cur;
# 上一层的剩余行、统计、准确度信息无用
delete from remain_column where node_id in (
    select id from node where level=_level
);
truncate table node2statis;
truncate table node2accur;

end $$

create procedure buildDT(
    column_names varchar(100),
    class_name varchar(20),
    max_level int
)
begin

drop table if exists node;
drop table if exists edge;
drop table if exists remain_column;
drop table if exists node2statis;
drop table if exists node2partition;
drop table if exists node2accur;
drop table if exists lvNode;

create table node(
    id int not null primary key,
    column_name varchar(20) not null default '',
    level int not null default 0,
    class int not null default 0
);
create table edge(
    from_id int not null,
    to_id int not null primary key auto_increment,
    column_value int not null,
    index edgeIdx (from_id,column_value)
);
create table remain_column(
    node_id int not null,
    column_name varchar(20) not null
);
create table node2statis(
    node_id int not null,
    column_name varchar(20) not null,
    column_value int not null,
    class int not null,
    count int not null default 0
);
create table node2accur(
    node_id int not null primary key,
    ins_count int not null default 0,
    max_count int not null default 0,
    accur double not null default 0,
    entro double not null default 0
);
create temporary table lvNode(
    id int not null primary key,
    column_name varchar(20)
);
set @sql = concat('create table node2partition(node_id int not null,'
       , replace(column_names, ',', ' int not null default 0,')
       , ' int not null default 0,' , class_name 
       , ' int not null, count int not null);');
prepare st from @sql;
execute st;

# 将传入的column_names拆分并设置为根节点包含的属性
set @split_count = (select length(column_names) - length(replace(column_names, ',', '')));
while @split_count >= 0 do
    insert into remain_column(node_id, column_name)
        select 0, substring_index(substring_index(column_names,',',@split_count + 1),',',-1);
    set @split_count = @split_count - 1;
end while;

# 创建根节点
insert into node(id, level)
    values (0, 0);
set @sql = concat('insert into node2partition(node_id,'
    ,column_names, ',', class_name, ',count) select 0,'
    ,column_names, ',', class_name,',count(*) from data group by '
    ,column_names,
    ',', class_name, ';');
prepare st from @sql;
execute st;

set @level = 0;

while (@level < max_level) and (select count(*) from node where level = @level) > 0 do
    # 计算关于列的统计，同时得到该层节点的分类
    call calcStatis(@level, class_name);

    # 得到下一个作为划分标准的行
    drop view if exists tmpEA;
    create view tmpEA as
        select rc.node_id, rc.column_name, entro-calcEntropy(rc.node_id, rc.column_name) as en 
        ,calcAccur(rc.node_id, rc.column_name)-accur as ac
        from remain_column as rc inner join node2accur as n2a on n2a.node_id = rc.node_id;
    update node
        inner join ( # 得到划分后熵增最大的行
            select node_id, column_name
            from tmpEA as tea 
            where ac>0 and en>0 and en>=all( # 前剪枝：划分后准确率增加
                select en from tmpEA
                where node_id=tea.node_id and ac>0 and en>0)
        ) te on te.node_id = node.id
        set node.column_name = te.column_name;

    # 列的每个值衍生出一条边和一个子节点
    call nodeSplit(@level);

    set @level = @level + 1;
end while;
if @level=max_level then
    call calcStatis(@level, class_name);
end if;

truncate table remain_column;
truncate table node2statis;
truncate table node2partition;
truncate table node2accur;
truncate table lvNode;

end $$

create procedure goNextLevel(
    _level int
)
begin

declare t_id int;
declare t_cn varchar(20);
declare flag int default 0;
# 选取位于该层，且存在预测节点的非叶子节点
declare cur cursor for 
    select n.id, n.column_name 
    from (select id, column_name from lvNode where column_name!='') as n
    inner join predictNode as pn on pn.node_id=n.id;
declare continue handler for not found set flag = 1;
open cur;
fetch cur into t_id, t_cn;
while flag != 1 do
    # 该结点上的预测节点都向下走一层

    set @sql = concat('update predictNode as pn inner join(select from_id,to_id,column_value from lvEdge where from_id='
        ,t_id,') as e on pn.node_id=e.from_id inner join(select id,',t_cn,' from predict) as p on p.'
        ,t_cn,'=e.column_value and p.id=pn.id set pn.node_id=e.to_id;');
    prepare st from @sql;
    execute st;

    fetch cur into t_id, t_cn;
end while;

end $$

create procedure predictDT(
    class_name varchar(20)
)
begin

drop table if exists predictNode;
drop table if exists predictResult;
drop table if exists lvNode;
drop table if exists lvEdge;

create table predictNode(
    id int not null primary key,
    node_id int not null default 0
);
create table predictResult(
    id int not null primary key,
    class int not null default 0
);
create temporary table lvNode(
    id int not null primary key,
    column_name varchar(20),
    class int
);
create temporary table lvEdge(
    from_id int not null,
    to_id int not null,
    column_value int,
    index lvEIdx (from_id, column_value)
);
insert into predictNode(id)
    select id from predict;
insert into predictResult(id)
    select id from predictNode;

set @level = 0;

while (select count(*) from predictNode)>0 and @level<5 do
    # 获取该层的非叶子节点
    truncate table lvNode;
    insert into lvNode(id,column_name,class)
        select id, column_name,class from node where level=@level;
    truncate table lvEdge;
    insert into lvEdge(from_id, to_id, column_value)
        select from_id, to_id, column_value
        from edge where from_id in(
            select id from lvNode
        );

    # 根据决策树走向下一级
    call goNextLevel(@level);

    # 若未走到下一级，则在该级进行预测
    update predictResult as p 
        inner join(
            select pn.id as p_id, lvNode.class 
            from predictNode as pn
            inner join lvNode on lvNode.id=pn.node_id
            ) as pr on p.id=pr.p_id
        set p.class=pr.class;

    # 删除预测完毕（仍在该层）的节点
    delete from predictNode where node_id in (
        select id from lvNode
    );

    set @level = @level + 1;

end while;

set @sql=concat('update predict as p inner join predictResult as pr on pr.id=p.id set p.',
    class_name,'=pr.class;');
prepare st from @sql;
execute st;

end $$

delimiter ;

# 运行样例
call buildDT('A,B,C', 'label', 5);
call predictDT('label');