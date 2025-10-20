# 阶段 1: 构建项目
# 使用一个包含 Maven 和 JDK 17 的镜像作为构建环境
FROM maven:3.9-eclipse-temurin-17 AS builder

# 设置工作目录
WORKDIR /app

# 复制整个项目代码到容器中
# 这样做是为了让 Maven 能够解析父 POM 和模块间的依赖
COPY . .

# --- NEW AND IMPORTANT PART ---
# Create the settings.xml file dynamically from environment variables.
# This command creates the .m2 directory and then writes the XML content into settings.xml.
# The shell will automatically substitute ${...} with the values of the environment variables.
RUN mkdir -p /root/.m2 && \
    cat <<EOF > /root/.m2/settings.xml
<settings>
  <servers>
    <server>
      <id>internal-mirror</id>
      <username>${MAVEN_REPO_USERNAME}</username>
      <password>${MAVEN_REPO_PASSWORD}</password>
    </server>
  </servers>
  <mirrors>
    <mirror>
      <id>internal-mirror</id>
      <url>${MAVEN_MIRROR_URL}</url>
      <mirrorOf>*</mirrorOf>
    </mirror>
  </mirrors>
</settings>
EOF

# 运行 Maven 命令来构建 "uber-jar"
# -pl mq-broker-server 指定只构建这个模块
# -am (also-make) 会同时构建它依赖的模块 (即 common 模块)
RUN mvn clean package -pl mq-broker-server -am

# ----------------------------------------------------------

# 阶段 2: 运行应用
# 使用一个轻量级的 JRE 镜像来运行应用，减小最终镜像体积
FROM eclipse-temurin:17-jre-jammy

# 设置工作目录
WORKDIR /app

# 从构建阶段(builder)复制最终的 "uber-jar" 到当前镜像中
# 注意文件名要和你 pom.xml 中 assembly 插件生成的一致
COPY --from=builder /app/mq-broker-server/target/mq-broker-server-1.1.0-jar-with-dependencies.jar ./app.jar

# 暴露 Broker Server 可能需要监听的端口
EXPOSE 5677

# 容器启动时运行的命令
# 使用 java -jar 命令来启动你的应用
CMD ["java", "-jar", "app.jar"]