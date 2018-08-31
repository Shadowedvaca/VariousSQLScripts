declare @StartStamp as datetime;
declare @UpperBound as tinyint;
declare @DBCursor as cursor;
declare @DBID as int
declare @TableCursor as cursor;
declare @TableID as int;
declare @TableSize as bigint;
declare @CurrentBucket as tinyint;
declare @CurrentSize as bigint;
declare @CurrentCount as int;
declare @NextSize as bigint;
declare @NextCount as int;
declare @FoundIt as bit;
set @StartStamp = getdate();
set @UpperBound = 6;

if object_ID('tempdb..#DBList','U') is not null
	drop table #DBList;

create table #DBList (
	DatabaseID int not null
	,RestoreType nvarchar(25) not null
	,LastUpdateStart datetime not null
	,LastUpdateEnd datetime null
);

if object_ID('tempdb..#LoadControl','U') is not null
	drop table #LoadControl;

create table #LoadControl (
	DatabaseID int not null
	,TableID int not null
	,LastSize bigint not null -- Rows X Columns
	,BucketID tinyint not null
	,LastUpdateStart datetime not null
	,LastUpdateEnd datetime null
);

if object_ID('tempdb..#Bucket','U') is not null
	drop table #Bucket;

create table #Bucket (
	ID int not null
	,ItemCount int not null
	,BucketSize int not null
);

insert into #DBList( DatabaseID, RestoreType, LastUpdateStart, LastUpdateEnd )
select
	ID
	,'Default Value'
	,@StartStamp
	,null
from
	inv.Databases


set @DBCursor = cursor for
	select DatabaseID from #DBList;

open @DBCursor;
fetch next from @DBCursor into @DBID;

while @@Fetch_Status = 0
begin

	insert into #LoadControl ( DatabaseID, TableID, LastSize, BucketID, LastUpdateStart, LastUpdateEnd )
	select
		t.DatabaseID
		,t.ID
		,isnull(t.SnapshotRowCount,1) * isnull(t.SnapshotColumnCount,1)
		,0
		,@StartStamp
		,null
	from
		inv.Tables t
	where
		t.DatabaseID = @DBID;

	with gen as ( select 1 as num union all select num + 1 as num from gen where num + 1 <= @UpperBound )
	insert into #Bucket ( ID, ItemCount, BucketSize )
	select num, 0, 0 from gen;

	set @TableCursor = cursor for
		select TableID, LastSize from #LoadControl where DatabaseID = @DBID order by LastSize desc;

	open @TableCursor;
	fetch next from @TableCursor into @TableID, @TableSize;

	while @@Fetch_Status = 0
	begin

		set @CurrentBucket = 0;
		set @FoundIt = 0;

		while @CurrentBucket <= @UpperBound
		begin
			
			-- First pass, skip and pull info from buckets
			if @CurrentBucket = 0
				set @FoundIt = 0;
			-- This is the last bucket, use it by default
			else if @CurrentBucket = @UpperBound
				set @FoundIt = 1;
			-- If the current bucket is still smaller than the next with the new table added, use it
			else if @CurrentSize + @TableSize < @NextSize
				set @FoundIt = 1;
			-- If the size is the same with this item added in, check the item counts to balance volume
			else if @CurrentSize + @TableSize = @NextSize
			begin
				-- If this would make an early bucket bigger, move it to the next one
				if @CurrentCount + 1 > @NextCount
					set @FoundIt = 0;
				-- Otherwise, use this one
				else
					set @FoundIt = 1;
			end
			-- Otherwise, check the next bucket
			else
				set @FoundIt = 0;

			if @FoundIt = 1
			begin
				update #Bucket set BucketSize += @TableSize, ItemCount += 1 where ID = @CurrentBucket;
				update #LoadControl set BucketID = @CurrentBucket where TableID = @TableID;
				set @CurrentBucket = @UpperBound + 1;
			end
			else
			begin
				set @CurrentBucket += 1;
				select @CurrentSize = isnull(BucketSize,0), @CurrentCount = isnull(ItemCount,0) from #Bucket where ID = @CurrentBucket;
				select @NextSize = isnull(BucketSize,0), @NextCount = isnull(ItemCount,0) from #Bucket where ID = @CurrentBucket + 1;
			end

		end;

		fetch next from @TableCursor into @TableID, @TableSize;
	
	end

	close @TableCursor;
	deallocate @TableCursor;

	fetch next from @DBCursor into @DBID;

end

close @DBCursor;
deallocate @DBCursor;

select
	*
from
	#Bucket