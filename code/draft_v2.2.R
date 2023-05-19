working_dir = '~/Sema4/etl/s4_curated_dataset_lci'
df = read.csv(paste0(working_dir, '/baseline_size_cat.csv'))

res = with(df, table(size_cat, size_date_cat))
result = res[c('<6mm(0.6cm)', '6-20mm(0.6-2cm)', '>20mm(2cm)'),c('<6M', '6M-1y', '1-3y', '>=3y')]
perc = result/rowSums(result)

df = read.csv(file.path(working_dir, 'baseline_number_cat.csv'))
res = with(df, table(cat, date_cat))
date_cat_order = c('<6M', '6M-1y', '1-3y', '>=3y')
cat_order=c('single', 'multiple')
result = res[cat_order, date_cat_order]
perc = result/rowSums(result)

tablename = 'baseline_ex_location'
df = read.csv(paste0(working_dir, '/', tablename, '.csv'))
res = with(df, table(cat, date_cat))
date_cat_order = c('<6M', '6M-1y', '1-3y', '>=3y')
cat_order='' #c('single', 'multiple')
result = res[, date_cat_order]
result
perc = result/rowSums(result)
perc
heatmaply(result, Rowv=NA, Colv=NA, scale='none')
#heatmaply(as.numeric(result))
require(pheatmap)
pheatmap(perc, cluster_rows=F,cluster_cols=F, Colv=NA, scale='none', display_numbers=T)

tablename = 'last_status_cat'
df = read.csv(paste0(working_dir, '/', tablename, '.csv'))
res = with(df, table(cat, date_cat))
date_cat_order = c('<6M', '6M-1y', '1-3y', '>=3y')
cat_order='' #c('single', 'multiple')
result = res[, date_cat_order]
result
perc = result/rowSums(result)
perc
heatmaply(result, Rowv=NA, Colv=NA, scale='none')
#heatmaply(as.numeric(result))
require(pheatmap)
pheatmap(perc, cluster_rows=F,cluster_cols=F, Colv=NA, scale='none', display_numbers=T, angle_col=45)

require(ggplot2)
df$months_before_dx = df$days_ggo_before_dx/30
p <- ggplot(df, aes(x=cat, y=months_before_dx)) +
  geom_violin(trim=T)
p = p + geom_dotplot(binaxis='y', stackdir='center', dotsize=.5, binwidth=1)
p + geom_hline(yintercept = c(6, 12, 36), color='blue')


p = ggplot(df, aes(x=months_before_dx, colour = cat)) + stat_ecdf()
p + geom_vline(xintercept = c(6, 12, 36), color='grey')
