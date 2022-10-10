# Sql splitter
This is a tool for splitting sql file generated from mysqldump command to small file.
Because when importing data via phpmyAdmin, we have to split the file smaller than 50MB.
But I imported sql file over 20MB, it will throw mysql timeout error.
So I split it into 20MB files, and merge small tables into one file that will reduce import operations.

當我從一個可以使用mysqldump指令的server轉移資料庫到一個只提供phpmyAdmin介面的伺服器時，我必須把700MB的檔案分割成許多的小檔案才能透過phpmyAdmin手動匯入.
因此做了這個小工具把超過20MB的table拆成多個檔案，把小於20MB的table合併成一個20MB的檔案減少操作次數.

// TODO：操作方式
