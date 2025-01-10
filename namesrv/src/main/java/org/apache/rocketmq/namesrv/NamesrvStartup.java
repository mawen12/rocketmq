/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

/**
 * Nameserver启动器
 */
public class NamesrvStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;
    private static ControllerConfig controllerConfig = null;

    /**
     * 本地启动配置：
     * <p>
     * Environment_Variables:
     * <ul>
     *     <li>ROCKETMQ_HOME=/Users/mawen/Documents/github/mawen12/rocketmq</li>
     * </ul>
     *
     * 启动{@link NamesrvController}，按需启动{@link ControllerManager}
     *
     * @param args
     */
    public static void main(String[] args) {
        /**
         * 读取配置文件，初始化配置，启动Namesrv控制器
         */
        main0(args);
        /**
         * 按需启动控制器管理器
         */
        controllerManagerMain();
    }

    public static NamesrvController main0(String[] args) {
        try {
            /**
             * 解析命令行参数和配置文件，用于填充如下配置文件：
             * <ul>
             *     <li>{@link namesrvConfig}</li>
             *     <li>{@link nettyServerConfig}</li>
             *     <li>{@link nettyClientConfig}</li>
             *     <li>{@link controllerConfig}</li>
             * </ul>
             */
            parseCommandlineAndConfigFile(args);
            /**
             * 创建并启动Namesrv控制器
             */
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static ControllerManager controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                /**
                 * 创建并启动控制器管理器
                 */
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        /**
         * 设置 ENV(rocketmq.remoting.version) 为 DEFAULT(MQVersion.CURRENT_VERSION)
         */
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        /**
         * 解析命令行参数
         */
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        /**
         * 如果未指定命令行参数，则退出
         */
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }

        /**
         * 初始化Namesrv配置、Netty服务端配置、Netty客户端配置
         */
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        /**
         * 服务端默认9876端口
         */
        nettyServerConfig.setListenPort(9876);
        /**
         * 将-c参数值解析出来
         */
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                /**
                 * 加载配置文件，并设置到属性文件中
                 */
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);
                /**
                 * 通过反射方式，从{@link properties}中读取属性填充{@link namesrvConfig}
                 */
                MixAll.properties2Object(properties, namesrvConfig);
                /**
                 * 通过反射方式，从{@link properties}中读取属性填充{@link nettyServerConfig}
                 */
                MixAll.properties2Object(properties, nettyServerConfig);
                /**
                 * 通过反射方式，从{@link properties}中读取属性填充{@link nettyClientConfig}
                 */
                MixAll.properties2Object(properties, nettyClientConfig);
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    /**
                     * 如果启动了Namesrv的控制器，则构造对应的{@link controllerConfig}
                     */
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    /**
                     * 通过反射方式，从{@link properties}中读取属性填充{@link controllerConfig}
                     */
                    MixAll.properties2Object(properties, controllerConfig);
                    /**
                     * 通过反射方式，从{@link properties}中读取属性填充{@link jraftConfig}
                     */
                    MixAll.properties2Object(properties, jraftConfig);
                }
                /**
                 * 将配置文件路径写入到{@link NamesrvConfig#configStorePath}
                 */
                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        /**
         * 使用命令行参数覆盖{@link namesrvConfig}
         */
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        /**
         * 解析-p参数，输出后退出
         */
        if (commandLine.hasOption('p')) {
            /**
             * 打印{@link namesrvConfig}到日志中
             */
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            /**
             * 打印{@link nettyServerConfig}到日志中
             */
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            /**
             * 打印{@link nettyClientConfig}到日志中
             */
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                /**
                 * 打印{@link controllerConfig}到日志中
                 */
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            /**
             * 退出系统
             */
            System.exit(0);
        }

        /**
         * 如果未制定rocketmq安装目录，退出系统
         */
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        /**
         * 打印{@link namesrvConfig}到日志中
         */
        MixAll.printObjectProperties(log, namesrvConfig);
        /**
         * 打印{@link namesrvConfig}到日志中
         */
        MixAll.printObjectProperties(log, nettyServerConfig);

    }

    public static NamesrvController createAndStartNamesrvController() throws Exception {
        /**
         * 创建Namesrv控制器
         */
        NamesrvController controller = createNamesrvController();
        /**
         * 启动控制器
         */
        start(controller);
        /**
         * 读取控制器的{@link nettyServerConfig}
         */
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        /**
         * 构造日志消息，并同时输出到日志和控制台中
         */
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controller;
    }

    public static NamesrvController createNamesrvController() {
        /**
         * 使用配置初始化Namesrv控制器
         */
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        /**
         * 记录所有属性，避免丢失
         */
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {
        /**
         * 校验控制器
         */
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        /**
         * 初始化控制器
         */
        boolean initResult = controller.initialize();
        /**
         * 如果初始化失败，则停止控制器，并推出系统
         */
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        /**
         * 添加JVM退出时，停止Namesrv控制器
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        /**
         * 初始化完成后，启动Namesrv控制器
         */
        controller.start();

        return controller;
    }

    public static ControllerManager createAndStartControllerManager() throws Exception {
        /**
         * 创建控制器管理器
         */
        ControllerManager controllerManager = createControllerManager();
        /**
         * 启动控制器管理器
         */
        start(controllerManager);
        /**
         * 构造日志消息，并同时输出到日志和控制台中
         */
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    public static ControllerManager createControllerManager() throws Exception {
        /**
         * 基于现有{@link nettyServerConfig}构造新的配置
         */
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        /**
         * 构造控制器配置
         */
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        /**
         * 记录所有属性，避免丢失
         */
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {
        /**
         * 校验控制器服务器
         */
        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        /**
         * 初始化控制器
         */
        boolean initResult = controllerManager.initialize();
        /**
         * 如果初始化失败，则停止控制器，并退出系统
         */
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        /**
         * 添加JVM退出时，停止控制器管理器
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        /**
         * 初始化完成后，启动控制器管理器
         */
        controllerManager.start();

        return controllerManager;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
