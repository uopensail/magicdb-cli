# Generated from magicdbParser.g4 by ANTLR 4.12.0
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .magicdbParser import magicdbParser
else:
    from magicdbParser import magicdbParser

# This class defines a complete listener for a parse tree produced by magicdbParser.
class magicdbParserListener(ParseTreeListener):

    # Enter a parse tree produced by magicdbParser#parse.
    def enterParse(self, ctx:magicdbParser.ParseContext):
        pass

    # Exit a parse tree produced by magicdbParser#parse.
    def exitParse(self, ctx:magicdbParser.ParseContext):
        pass


    # Enter a parse tree produced by magicdbParser#command_list.
    def enterCommand_list(self, ctx:magicdbParser.Command_listContext):
        pass

    # Exit a parse tree produced by magicdbParser#command_list.
    def exitCommand_list(self, ctx:magicdbParser.Command_listContext):
        pass


    # Enter a parse tree produced by magicdbParser#command.
    def enterCommand(self, ctx:magicdbParser.CommandContext):
        pass

    # Exit a parse tree produced by magicdbParser#command.
    def exitCommand(self, ctx:magicdbParser.CommandContext):
        pass


    # Enter a parse tree produced by magicdbParser#database_cmd.
    def enterDatabase_cmd(self, ctx:magicdbParser.Database_cmdContext):
        pass

    # Exit a parse tree produced by magicdbParser#database_cmd.
    def exitDatabase_cmd(self, ctx:magicdbParser.Database_cmdContext):
        pass


    # Enter a parse tree produced by magicdbParser#drop_database.
    def enterDrop_database(self, ctx:magicdbParser.Drop_databaseContext):
        pass

    # Exit a parse tree produced by magicdbParser#drop_database.
    def exitDrop_database(self, ctx:magicdbParser.Drop_databaseContext):
        pass


    # Enter a parse tree produced by magicdbParser#show_databases.
    def enterShow_databases(self, ctx:magicdbParser.Show_databasesContext):
        pass

    # Exit a parse tree produced by magicdbParser#show_databases.
    def exitShow_databases(self, ctx:magicdbParser.Show_databasesContext):
        pass


    # Enter a parse tree produced by magicdbParser#create_database.
    def enterCreate_database(self, ctx:magicdbParser.Create_databaseContext):
        pass

    # Exit a parse tree produced by magicdbParser#create_database.
    def exitCreate_database(self, ctx:magicdbParser.Create_databaseContext):
        pass


    # Enter a parse tree produced by magicdbParser#machine_cmd.
    def enterMachine_cmd(self, ctx:magicdbParser.Machine_cmdContext):
        pass

    # Exit a parse tree produced by magicdbParser#machine_cmd.
    def exitMachine_cmd(self, ctx:magicdbParser.Machine_cmdContext):
        pass


    # Enter a parse tree produced by magicdbParser#show_machines.
    def enterShow_machines(self, ctx:magicdbParser.Show_machinesContext):
        pass

    # Exit a parse tree produced by magicdbParser#show_machines.
    def exitShow_machines(self, ctx:magicdbParser.Show_machinesContext):
        pass


    # Enter a parse tree produced by magicdbParser#delete_machine.
    def enterDelete_machine(self, ctx:magicdbParser.Delete_machineContext):
        pass

    # Exit a parse tree produced by magicdbParser#delete_machine.
    def exitDelete_machine(self, ctx:magicdbParser.Delete_machineContext):
        pass


    # Enter a parse tree produced by magicdbParser#add_machine.
    def enterAdd_machine(self, ctx:magicdbParser.Add_machineContext):
        pass

    # Exit a parse tree produced by magicdbParser#add_machine.
    def exitAdd_machine(self, ctx:magicdbParser.Add_machineContext):
        pass


    # Enter a parse tree produced by magicdbParser#table_cmd.
    def enterTable_cmd(self, ctx:magicdbParser.Table_cmdContext):
        pass

    # Exit a parse tree produced by magicdbParser#table_cmd.
    def exitTable_cmd(self, ctx:magicdbParser.Table_cmdContext):
        pass


    # Enter a parse tree produced by magicdbParser#show_tables.
    def enterShow_tables(self, ctx:magicdbParser.Show_tablesContext):
        pass

    # Exit a parse tree produced by magicdbParser#show_tables.
    def exitShow_tables(self, ctx:magicdbParser.Show_tablesContext):
        pass


    # Enter a parse tree produced by magicdbParser#drop_table.
    def enterDrop_table(self, ctx:magicdbParser.Drop_tableContext):
        pass

    # Exit a parse tree produced by magicdbParser#drop_table.
    def exitDrop_table(self, ctx:magicdbParser.Drop_tableContext):
        pass


    # Enter a parse tree produced by magicdbParser#create_table.
    def enterCreate_table(self, ctx:magicdbParser.Create_tableContext):
        pass

    # Exit a parse tree produced by magicdbParser#create_table.
    def exitCreate_table(self, ctx:magicdbParser.Create_tableContext):
        pass


    # Enter a parse tree produced by magicdbParser#desc_table.
    def enterDesc_table(self, ctx:magicdbParser.Desc_tableContext):
        pass

    # Exit a parse tree produced by magicdbParser#desc_table.
    def exitDesc_table(self, ctx:magicdbParser.Desc_tableContext):
        pass


    # Enter a parse tree produced by magicdbParser#version_cmd.
    def enterVersion_cmd(self, ctx:magicdbParser.Version_cmdContext):
        pass

    # Exit a parse tree produced by magicdbParser#version_cmd.
    def exitVersion_cmd(self, ctx:magicdbParser.Version_cmdContext):
        pass


    # Enter a parse tree produced by magicdbParser#show_versions.
    def enterShow_versions(self, ctx:magicdbParser.Show_versionsContext):
        pass

    # Exit a parse tree produced by magicdbParser#show_versions.
    def exitShow_versions(self, ctx:magicdbParser.Show_versionsContext):
        pass


    # Enter a parse tree produced by magicdbParser#show_current_version.
    def enterShow_current_version(self, ctx:magicdbParser.Show_current_versionContext):
        pass

    # Exit a parse tree produced by magicdbParser#show_current_version.
    def exitShow_current_version(self, ctx:magicdbParser.Show_current_versionContext):
        pass


    # Enter a parse tree produced by magicdbParser#update_version.
    def enterUpdate_version(self, ctx:magicdbParser.Update_versionContext):
        pass

    # Exit a parse tree produced by magicdbParser#update_version.
    def exitUpdate_version(self, ctx:magicdbParser.Update_versionContext):
        pass


    # Enter a parse tree produced by magicdbParser#drop_version.
    def enterDrop_version(self, ctx:magicdbParser.Drop_versionContext):
        pass

    # Exit a parse tree produced by magicdbParser#drop_version.
    def exitDrop_version(self, ctx:magicdbParser.Drop_versionContext):
        pass


    # Enter a parse tree produced by magicdbParser#load_data.
    def enterLoad_data(self, ctx:magicdbParser.Load_dataContext):
        pass

    # Exit a parse tree produced by magicdbParser#load_data.
    def exitLoad_data(self, ctx:magicdbParser.Load_dataContext):
        pass


    # Enter a parse tree produced by magicdbParser#select_data.
    def enterSelect_data(self, ctx:magicdbParser.Select_dataContext):
        pass

    # Exit a parse tree produced by magicdbParser#select_data.
    def exitSelect_data(self, ctx:magicdbParser.Select_dataContext):
        pass


    # Enter a parse tree produced by magicdbParser#properties.
    def enterProperties(self, ctx:magicdbParser.PropertiesContext):
        pass

    # Exit a parse tree produced by magicdbParser#properties.
    def exitProperties(self, ctx:magicdbParser.PropertiesContext):
        pass


    # Enter a parse tree produced by magicdbParser#table.
    def enterTable(self, ctx:magicdbParser.TableContext):
        pass

    # Exit a parse tree produced by magicdbParser#table.
    def exitTable(self, ctx:magicdbParser.TableContext):
        pass


    # Enter a parse tree produced by magicdbParser#database_name.
    def enterDatabase_name(self, ctx:magicdbParser.Database_nameContext):
        pass

    # Exit a parse tree produced by magicdbParser#database_name.
    def exitDatabase_name(self, ctx:magicdbParser.Database_nameContext):
        pass


    # Enter a parse tree produced by magicdbParser#pair.
    def enterPair(self, ctx:magicdbParser.PairContext):
        pass

    # Exit a parse tree produced by magicdbParser#pair.
    def exitPair(self, ctx:magicdbParser.PairContext):
        pass


    # Enter a parse tree produced by magicdbParser#value.
    def enterValue(self, ctx:magicdbParser.ValueContext):
        pass

    # Exit a parse tree produced by magicdbParser#value.
    def exitValue(self, ctx:magicdbParser.ValueContext):
        pass



del magicdbParser