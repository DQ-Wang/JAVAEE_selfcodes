//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoredis;

import cn.edu.xmu.javaee.core.infrastructure.RedisUtil;
import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.UserToken;
import cn.edu.xmu.javaee.core.util.JacksonUtil;
import cn.edu.xmu.javaee.productdemoredis.service.vo.CustomerProductVo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.transaction.annotation.Transactional;

import static cn.edu.xmu.javaee.core.model.Constants.PLATFORM;
import static org.hamcrest.CoreMatchers.is;

@SpringBootTest(classes = ProductDemoRedisApplication.class)
@AutoConfigureMockMvc
@Transactional
public class AdminProductControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RedisUtil redisUtil;

    private static final String PRODUCTID = "/shops/{shopId}/products/{id}";
    private static final String PRODUCT = "/shops/{shopId}/products";

    @Test
    public void getProduct() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();
        this.mockMvc.perform(MockMvcRequestBuilders.get(PRODUCTID, 10,1550)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.name", is("欢乐家久宝桃罐头")));
        //.andDo(MockMvcResultHandlers.print());

    }

    @Test
    public void searchProductByName() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();
        this.mockMvc.perform(MockMvcRequestBuilders.get(PRODUCT,6).contentType("application/json;charset=UTF-8")
                        .param("name", "奥利奥缤纷双果味")
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.length()").value(1))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[?(@.id== '%d' )].name", 1559).value("奥利奥缤纷双果味"));
        //.andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void searchProductByNameGivenWrongShopId() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();

        this.mockMvc.perform(MockMvcRequestBuilders.get(PRODUCT,2).contentType("application/json;charset=UTF-8")
                        .param("name", "奥")
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.length()").value(0));
        //.andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void createProduct() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();

        String body = "{\"name\":\"水果糖\",\"originalPrice\":100,\"weight\":807,\"barcode\":\"1234455\",\"unit\":\"盒\",\"originPlace\":\"长沙\"}";
        String ret = this.mockMvc.perform(MockMvcRequestBuilders.post(PRODUCT,2)
                        .content(body.getBytes("utf-8"))
                        .contentType(MediaType.APPLICATION_JSON_VALUE)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.CREATED.getErrNo())))
                //.andDo(MockMvcResultHandlers.print())
                .andReturn().getResponse().getContentAsString();

        CustomerProductVo retObj = JacksonUtil.parseObject(ret, "data", CustomerProductVo.class);

        this.mockMvc.perform(MockMvcRequestBuilders.get(PRODUCTID, 2, retObj.getId()).header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.name", is("水果糖")));
        //.andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void modiProduct() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();

        String body = "{\"name\":\"奶糖\",\"originalPrice\":200}";
        this.mockMvc.perform(MockMvcRequestBuilders.put(PRODUCTID,1, 1580)
                        .content(body.getBytes("utf-8"))
                        .contentType(MediaType.APPLICATION_JSON_VALUE)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())));
                //.andDo(MockMvcResultHandlers.print());

        this.mockMvc.perform(MockMvcRequestBuilders.get(PRODUCTID, 1, 1580)
                .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.name", is("奶糖")))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.originalPrice", is(200)));
                //.andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void modiProductGivenNonExistId() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();

        String body = "{\"name\":\"奶糖\",\"originalPrice\":200}";
        this.mockMvc.perform(MockMvcRequestBuilders.put(PRODUCTID, 2,158011)
                        .content(body.getBytes("utf-8"))
                        .contentType(MediaType.APPLICATION_JSON_VALUE)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isNotFound());
                //.andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void delProduct() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();

        this.mockMvc.perform(MockMvcRequestBuilders.delete(PRODUCTID, 1,1580)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.errno", is(ReturnNo.OK.getErrNo())));
        //.andDo(MockMvcResultHandlers.print());

        this.mockMvc.perform(MockMvcRequestBuilders.get(PRODUCTID, 1,1580)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isNotFound());
        //.andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void delProductGivenNonExistId() throws Exception {
        UserToken user = UserToken.builder().id(2L).name("admin2").departId(PLATFORM).build();

        this.mockMvc.perform(MockMvcRequestBuilders.delete(PRODUCTID, 2,1580112)
                        .header("Authorization", JacksonUtil.toJson(user)))
                .andExpect(MockMvcResultMatchers.status().isNotFound());
        //.andDo(MockMvcResultHandlers.print());
    }

}