/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

bool isFileEmpty(const std::string &file_name) {
    std::ifstream file(file_name,std::ios::ate|std::ios::binary);
    return file.tellg()==0;
}
/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    // 切换目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    // 打开数据库的元数据文件，并读取元数据
    std::ifstream ifs(DB_META_NAME);
    if (!ifs) {
        throw UnixError();
    }
    ifs >> db_;
    // std::cout<<"there are "<<db_.tabs_.size()<<" tables\n";
    // 加载每个表的元数据和记录文件
    for (auto &entry : db_.tabs_) {
        const std::string &tab_name = entry.first;
        // 打开记录文件
        std::unique_ptr<RmFileHandle> fh = rm_manager_->open_file(tab_name);
        if (!fh) {
            throw UnixError();
        }
        // 保存记录文件句柄
        fhs_[tab_name] = std::move(fh);
//        std::cout<<"index num="<<entry.second.indexes.size()<<"\n";
        std::vector<std::vector<std::string>>col_names;
        for(auto index:entry.second.indexes) {
            std::vector<std::string> col_name;
            for(const auto& col:index.cols){
                col_name.emplace_back(col.name);
            }
            col_names.push_back(col_name);
        }
        for(auto& col_name:col_names){
            std::unique_ptr<IxIndexHandle> ih = ix_manager_->open_index(tab_name,col_name);
            ihs_[ix_manager_->get_index_name(tab_name,col_name)] = std::move(ih);
                drop_index(tab_name, col_name, nullptr);
                create_index(tab_name, col_name, nullptr);
        }
    }
//    std::cout<<"fhs.size="<<fhs_.size()<<"\n";
//    std::cout<<"ihs.size="<<ihs_.size()<<"\n";
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // 切换到数据库目录
    /*
    if (chdir(db_.name_.c_str()) < 0) {
        throw UnixError();
    }
    */
    // 将元数据写到DB_META
    flush_meta();
    // 关闭所有打开的表的文件
    for (auto &entry : fhs_) {
        const std::string &tab_name = entry.first;
        RmFileHandle *fh = entry.second.get();
        // 将表的数据写回磁盘

        // 关闭表的文件
        rm_manager_->close_file(fh);
    }
    for(auto &entry : ihs_) {
        const std::string &tab_name = entry.first;
        IxIndexHandle *ih = entry.second.get();
        ix_manager_->close_index(ih);
    }

    db_.tabs_.clear();
    db_.name_.clear();

    // 清除所有与该数据库相关的内存数据结构
    db_ = DbMeta();  // 使用默认构造函数创建一个新的 DbMeta 对象，替换旧的 db_
    fhs_.clear();  // 清除所有文件句柄
    ihs_.clear();
    std::ofstream ofs;
    ofs.open(LOG_FILE_NAME, std::ofstream::out | std::ofstream::trunc);
    ofs.close();
    // 切换回原来的目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {

    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if(!db_.is_table(tab_name)){
        throw TableNotFoundError(tab_name);
    }
    // 如果存在文件句柄，关闭并删除
    if (fhs_.find(tab_name) != fhs_.end()) {
        rm_manager_->close_file(fhs_.at(tab_name).get());
        fhs_.erase(tab_name);
    } else {
        // 如果不存在文件句柄，抛出错误或直接返回
        // throw SomeError();
        return;
    }
    // rm_manager_->close_file(fhs_.at(tab_name).get());
    rm_manager_->destroy_file(tab_name);

    db_.tabs_.erase(tab_name);

    // 如果有这个表的文件句柄，从文件句柄的映射中删除
    fhs_.erase(tab_name);

    // 如果有这个表的索引句柄，从索引句柄的映射中删除
    ihs_.erase(tab_name);

    // 将更改写回磁盘
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */


void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    if(ix_manager_->exists(tab_name,col_names)){
        throw IndexExistsError(tab_name,col_names);
    }
    // 构造key的必要信息
    size_t key_size=0;
    std::vector<ColMeta> index_cols;
    for(const auto& col_name : col_names){
        auto col=tab.get_col(col_name);
        index_cols.push_back(*col);
        key_size+= col->len;
    }
    auto index_meta = ix_manager_->create_index(tab_name,index_cols);
    tab.indexes.push_back(index_meta);
    auto ih = ix_manager_->open_index(tab_name,col_names);
    assert(ih!= nullptr);
    std::vector<Rid> rid_to_delete;
    auto fh = fhs_.at(tab_name).get();
    for (RmScan rm_scan(fh); !rm_scan.is_end(); rm_scan.next()) {
        auto rec = fh->get_record(rm_scan.rid(),context);
        char key[key_size+1];
        memset(key,0,sizeof(key));
        int offset=0;
        for(auto& col_name:col_names) {
            auto col = tab.get_col(col_name);
            memcpy(key+offset, rec->data+col->offset, col->len);
            offset+=col->len;
        }
        // if not unique, remove record
        bool unique=ih->insert_entry(key, rm_scan.rid(), nullptr);
        if(!unique){
            RmRecord rmRecord(rec->size,rec->data);
            rid_to_delete.emplace_back(rm_scan.rid());
            auto deleteLogRecord = LogRecord(context->txn_->get_transaction_id(),context->txn_->get_prev_lsn(),
                                             LogType::DELETE,rm_scan.rid(),rmRecord,tab_name);
            context->txn_->set_prev_lsn(context->log_mgr_->add_log_to_buffer(&deleteLogRecord));
        } else{
            auto txn= context->txn_;
            LogRecord index_insert_log_record=LogRecord(txn->get_transaction_id(),txn->get_prev_lsn(),LogType::INSERT_ENTRY,
                                                        rm_scan.rid(),key,key_size,ix_manager_->get_index_name(tab_name,col_names));
            txn->set_prev_lsn(context->log_mgr_->add_log_to_buffer(&index_insert_log_record));
        }
    }
    // remove record
    for (auto & rid:rid_to_delete){
        fhs_[tab_name]->delete_record(rid,context);
    }
    // Store index handle
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    assert(ihs_.count(index_name) == 0);
    ihs_[index_name] = std::move(ih);
    flush_meta();
//    std::cout<<"create index "<<index_name<<" successfully\n";
    ihs_.at(index_name).get();


}


void SmManager::show_index(const std::string&tab_name, Context*context){
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    TabMeta& tab=db_.get_table(tab_name);
    auto indexes = tab.indexes;

    if(indexes.empty()){
        outfile.close();
        return;
    }
    for (auto& index : indexes) {
        // 构造
        std::string index_name="(";
        for (auto& col:index.cols) {
            index_name+=col.name;
            index_name += ',';
        }
        index_name = index_name.substr(0,index_name.size()-1);
        index_name+=")";
        // 输出
        outfile<<"| "<<tab_name<<" | unique | "<<index_name<<" |\n";
    }
    outfile.close();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    if(!ix_manager_->exists(tab_name,col_names)){
        throw IndexNotFoundError(tab_name,col_names);
    }
    auto index_name = ix_manager_->get_index_name(tab_name,col_names);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name,col_names);
    ihs_.erase(index_name);
    auto indexMeta_iter = tab.get_index_meta(col_names);
    tab.indexes.erase(indexMeta_iter);
    flush_meta();
//    std::cout<<"drop index "<<index_name<<" successfully\n";
}
/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    std::vector<std::string> col_names;
    for(auto col:cols){
        col_names.push_back(col.name);
    }
    if(!ix_manager_->exists(tab_name,cols)){
        throw IndexNotFoundError(tab_name,col_names);
    }
    auto index_name = ix_manager_->get_index_name(tab_name,cols);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name,cols);
    ihs_.erase(index_name);
    auto indexMeta_iter = tab.get_index_meta(col_names);
    tab.indexes.erase(indexMeta_iter);
    flush_meta();
//    std::cout<<"drop index "<<index_name<<" successfully\n";
}
