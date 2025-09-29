# 📝 Notas da API do Kestra - Evino

## 🔑 **Credenciais da API**

### **Endpoint Base**
```
https://api.evino.com.br/kestra/api/v1/main/flows
```

### **API Key**
```
X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP
```

### **Headers Necessários**
```http
X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP
Content-Type: application/json
Accept: application/json
User-Agent: MonitorDW/1.0
```

## 🏢 **Configurações do Sistema**

### **Tenant**
```
main
```

### **Namespace**
```
rpa.varejofacil
```

### **Flow IDs Identificados**
- `rpa_vf_clientes_ev`
- `rpa_vf_clientes_franquias_ev`
- `rpa_vf_clientes_franquias_gc`
- Total: 14 flows no namespace `rpa.varejofacil`

## 🔗 **Endpoints da API**

### **1. Listar Flows**
```http
GET https://api.evino.com.br/kestra/api/v1/main/flows
```

### **2. Listar Execuções**
```http
GET https://api.evino.com.br/kestra/api/v1/main/executions
```

### **3. Execuções de um Flow Específico**
```http
GET https://api.evino.com.br/kestra/api/v1/main/executions/flows/{namespace}/{flowId}
```

**Exemplo:**
```http
GET https://api.evino.com.br/kestra/api/v1/main/executions/flows/rpa.varejofacil/rpa_vf_clientes_ev
```

### **4. Detalhes de uma Execução**
```http
GET https://api.evino.com.br/kestra/api/v1/main/executions/{executionId}
```

### **5. Listar Namespaces**
```http
GET https://api.evino.com.br/kestra/api/v1/main/namespaces
```

## 🧪 **Comandos de Teste (cURL)**

### **Testar Listagem de Flows**
```bash
curl -H "X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json" \
     "https://api.evino.com.br/kestra/api/v1/main/flows"
```

### **Testar Execuções de um Flow**
```bash
curl -H "X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json" \
     "https://api.evino.com.br/kestra/api/v1/main/executions/flows/rpa.varejofacil/rpa_vf_clientes_ev"
```

### **Testar Listagem de Execuções**
```bash
curl -H "X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json" \
     "https://api.evino.com.br/kestra/api/v1/main/executions"
```

## 📊 **Status dos Testes**

### **✅ O que está funcionando:**
- Servidor responde (não é problema de conectividade)
- API Key é reconhecida (não é mais 401)
- Endpoints existem (não é mais 404)

### **❌ Problema atual:**
- **Status HTTP**: `403 Forbidden` (Acesso negado)
- **Causa**: API Key sem permissões para acessar os endpoints
- **Solução**: Solicitar permissões ao administrador do Kestra

## 🔧 **Configuração no secrets.toml**

```toml
[kestra]
base_url = "https://api.evino.com.br/kestra"
api_key = "ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP"
tenant = "main"
```

## 📋 **Informações para o Administrador do Kestra**

### **O que solicitar:**
1. **Permissões de leitura** para a API Key
2. **Acesso aos endpoints** de flows e execuções
3. **Confirmação** de que a API Key pode acessar o namespace `rpa.varejofacil`

### **Endpoints que precisam de permissão:**
- `GET /api/v1/main/flows` - Para listar flows
- `GET /api/v1/main/executions` - Para listar execuções
- `GET /api/v1/main/executions/flows/{namespace}/{flowId}` - Para execuções específicas
- `GET /api/v1/main/namespaces` - Para listar namespaces

### **API Key:**
```
ELFkM8LTsLpTmbvzVyVqCslVDfBdNACP
```

### **Namespace de interesse:**
```
rpa.varejofacil
```

### **Flow IDs de interesse:**
```
rpa_vf_clientes_ev
rpa_vf_clientes_franquias_ev
rpa_vf_clientes_franquias_gc
```

## 🎯 **Próximos Passos**

1. **Contatar o administrador do Kestra da Evino**
2. **Solicitar permissões** para a API Key
3. **Confirmar acesso** ao namespace `rpa.varejofacil`
4. **Testar novamente** após obter permissões
5. **Implementar monitoramento** no Monitor DW

## 📚 **Documentação de Referência**

- **Documentação oficial do Kestra**: https://kestra.io/docs/api-reference/open-source
- **Endpoint específico**: `GET /api/v1/{tenant}/executions/flows/{namespace}/{flowId}`

---

**Data de criação**: 22/09/2025  
**Status**: Aguardando permissões do administrador do Kestra  
**Próxima ação**: Contatar administrador para solicitar permissões da API Key
