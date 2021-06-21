package edu.harvard.i2b2.common.util.db;

import org.apache.commons.lang3.StringUtils;

public class QueryUtil {

    public static String getOperatorByValue(String compareValue) {
        if(compareValue.startsWith("'") && compareValue.endsWith("'"))
            compareValue = compareValue.substring(1, compareValue.length() - 1);
        if(compareValue.startsWith("%") && compareValue.endsWith("%"))
            return "[";
        else if(!compareValue.startsWith("%") && compareValue.endsWith("%"))
            return "%STARTSWITH";
        return "like";
    }

    public static String getCleanValue(String compareValue) {
        if(compareValue.startsWith("'") && compareValue.endsWith("'")) {
            compareValue = compareValue.substring(1, compareValue.length() - 1);
            if (compareValue.startsWith("%") && compareValue.endsWith("%"))
                compareValue = compareValue.substring(1, compareValue.length() - 1);
            else if (!compareValue.startsWith("%") && compareValue.endsWith("%"))
                compareValue = compareValue.substring(0, compareValue.length() - 1);
            return "'" + compareValue + "'";
        } else {
            if (compareValue.startsWith("%") && compareValue.endsWith("%"))
                return compareValue.substring(1, compareValue.length() - 1);
            else if (!compareValue.startsWith("%") && compareValue.endsWith("%"))
                return compareValue.substring(0, compareValue.length() - 1);
            return compareValue;
        }
    }

    public static String buildTableAlias(String tableAlias) {
        if (tableAlias != null && tableAlias.trim().length() > 0) {
            if(!tableAlias.trim().endsWith("."))
                tableAlias += ".";
        } else
            tableAlias = StringUtils.EMPTY;
        return tableAlias;
    }
}
