public class match_sql_comment {
    public static void main(String[] args) {
        String sql = """
                    /*
                    * This is a sql
                    */
                    select *
                        from tmp.dws_aaa_df
                        where dt = "20250501"
                        -- 过滤xxx
                        and id = 1
                        and status = 0 -- only take this one
                        group by id -- ddd
                """;

        // 消除注释
        String sql1 = sql.replaceAll("--.*?($|\\n)", " ");
        // 消除多行注释
        String sql2 = sql.replaceAll("/\\*[\\s\\S]*?\\*/", " ");
        System.out.println(sql1);
        System.out.println("--------------------------------");
        System.out.println(sql2);
    }
}