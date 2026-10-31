package com.tsa.mqmonitor.config;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.formLogin;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.authenticated;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.unauthenticated;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tsa.mqmonitor.support.PostgresTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

class SecurityConfigTest extends PostgresTestBase {

    @Autowired
    MockMvc mvc;

    @Test
    void anonymousIsRedirectedToLogin() throws Exception {
        mvc.perform(get("/connections"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
    }

    @Test
    void anonymousStreamGets401NotARedirect() throws Exception {
        mvc.perform(get("/connections/1/stream")).andExpect(status().isUnauthorized());
        mvc.perform(get("/connections/stream")).andExpect(status().isUnauthorized());
    }

    @Test
    void loginPageIsPublic() throws Exception {
        mvc.perform(get("/login")).andExpect(status().isOk());
    }

    @Test
    void prometheusIsPublic() throws Exception {
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk());
    }

    @Test
    void adminCanLogIn() throws Exception {
        mvc.perform(formLogin().user("admin").password("admin-pass")).andExpect(authenticated());
    }

    @Test
    void wrongPasswordIsRejected() throws Exception {
        mvc.perform(formLogin().user("admin").password("nope")).andExpect(unauthenticated());
    }
}
