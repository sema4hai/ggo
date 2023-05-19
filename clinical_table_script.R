# Function responsible for creating clinical table (stratified by group.variable)
# over a series of predictor variables (predictors)
create_clinical_table = function(df, predictors, group.variable, percentage.flag, clinical.table.title,
                                 clinical.table.file.name, ...){

  for(i in 1:length(predictors)){
    # Patient Total by Group and Predictor Strata Levels
    clinical.summary = df %>%
      select(predictors, group.variable) %>%
      group_by(across({{group.variable}})) %>%
      count(.data[[predictors[i]]]) %>%
      mutate(Variable = predictors[i])

    clinical.summary.count.table = clinical.summary %>%
      select(-Variable) %>%
      spread(group.variable, n)

    clinical.summary.count.table[is.na(clinical.summary.count.table)] = 0

    if(percentage.flag == "columns"){
      # Patient Totals by Group Variable Strata Levels
      clinical.subgroup.total = df %>%
        group_by(across({{group.variable}})) %>%
        summarise(Subgroup.Count = n()) %>%
        ungroup()

      clinical.summary = clinical.summary %>%
        inner_join(clinical.subgroup.total, by = group.variable) %>%
        mutate(Percentage = round(n / Subgroup.Count * 100, digits = 0)) %>%
        mutate(Count.String = paste(n, " (", Percentage, "%)", sep = "")) %>%
        select(-n) %>%
        rename(n = Count.String)

    }else{
      clinical.subgroup.total = df %>%
        group_by(.data[[predictors[i]]]) %>%
        summarise(Subgroup.Count = n()) %>%
        ungroup()

      clinical.summary = clinical.summary %>%
        inner_join(clinical.subgroup.total, by = predictors[i]) %>%
        mutate(Percentage = round(n / Subgroup.Count * 100, digits = 0)) %>%
        mutate(Count.String = paste(n, " (", Percentage, "%)", sep = "")) %>%
        select(-n) %>%
        rename(n = Count.String)
    }


    # Perform Chi-Square Test when sample size for all cells is sufficiently large
    if(!any(clinical.summary.count.table[, -1] == 0) & !(any(clinical.summary.count.table[, -1] < 20) & nrow(clinical.summary.count.table) == 2)){

      chi.squared.test = chisq.test(clinical.summary.count.table[, -1])

      chi.squared.test.p.value = signif(chi.squared.test$p.value, digits = 3)

      clinical.summary$Variable = paste(clinical.summary$Variable, " (p = ",
                                        chi.squared.test.p.value, ")", sep = "")

    # Perform Fisher's Exact Test when sample size is insufficient for chi-squared test
    }else{
      if(!any(clinical.summary.count.table[, -1] == 0) & nrow(clinical.summary.count.table) == 2){
        fisher.test = fisher.test(clinical.summary.count.table[, -1])

        fisher.test.p.value = signif(fisher.test$p.value, digits = 3)

        clinical.summary$Variable = paste(clinical.summary$Variable, " (p = ",
                                          fisher.test.p.value, ")", sep = "")
      }
    }

    names(clinical.summary)[2] = "Value"

    clinical.summary = select(clinical.summary, group.variable,
                              Variable, Value, n)

    if(i == 1){
      clinical.summary.table = clinical.summary
    }else{
      clinical.summary.table = rbind.data.frame(clinical.summary.table,
                                                clinical.summary)
    }
  }

  clinical.summary.table = pivot_wider(clinical.summary.table,
    id_cols = c(group.variable, Variable, Value), names_from = group.variable, values_from = n)

  clinical.summary.table = clinical.summary.table %>%
    gt(rowname_col = "Value", groupname_col = "Variable") %>%
    tab_header(title = clinical.table.title)

  gtsave(clinical.summary.table, clinical.table.file.name)

  return(clinical.summary.table)

}

#clinical.subgroup.table = create_clinical_table(dataframe, predictors, group, "rows", table.name,
#                                                table.file.name)
