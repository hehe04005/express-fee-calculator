from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
import os
import uuid
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['RESULTS_FOLDER'] = 'results'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

class ExpressCalculator:
    def __init__(self):
        self.wheat_prices = {
            '10.5kg': {'province_in': 5.5, 'province_out': 7.0},
            '12.5kg': {'province_in': 5.9, 'province_out': 7.7},
            '21kg': {'province_in': 8.2, 'province_out': 11.3}
        }
        
        self.corn_prices = {
            'tier1': {
                'provinces': ['北京市', '上海市'],
                'continuation_fee': 0.6,
                'prices': {'0-0.5kg': 2.60, '0.5-1kg': 2.80, '1.01-2kg': 3.40, '2.01-3kg': 4.20, '3kg+': 5.00}
            },
            'tier2': {
                'provinces': ['江苏省', '浙江省', '河南省', '广东省', '深圳市', '陕西省', '山西省', '山东省', '湖北省', '湖南省', '天津市', '安徽省', '河北省'],
                'continuation_fee': 0.6,
                'prices': {'0-0.5kg': 1.60, '0.5-1kg': 1.80, '1.01-2kg': 2.40, '2.01-3kg': 3.20, '3kg+': 4.00}
            },
            'tier3': {
                'provinces': ['福建省', '辽宁省', '重庆市', '四川省', '贵州省', '吉林省', '云南省', '黑龙江省', '甘肃省', '广西壮族自治区', '宁夏回族自治区', '青海省', '海南省'],
                'continuation_fee': 1.1,
                'prices': {'0-0.5kg': 1.60, '0.5-1kg': 1.80, '1.01-2kg': 2.40, '2.01-3kg': 3.20, '3kg+': 4.00}
            },
            'special': {
                '内蒙古自治区': {'base': 6, 'continuation': 4},
                '新疆维吾尔自治区': {'base': 18, 'continuation': 18},
                '西藏自治区': {'base': 18, 'continuation': 18}
            }
        }
    
    def get_wheat_price(self, weight, province):
        weight = float(weight)
        is_in_province = '河南' in str(province)
        
        if weight <= 11:
            tier = '10.5kg'
        elif weight >= 12 and weight <= 13:
            tier = '12.5kg'
        elif weight >= 20 and weight <= 22:
            tier = '21kg'
        elif weight < 10:
            tier = '10.5kg'
        elif weight > 13:
            tier = '21kg' if weight >= 20 else '12.5kg'
        else:
            return 0, '未定义'
        
        price_key = 'province_in' if is_in_province else 'province_out'
        price = self.wheat_prices[tier][price_key]
        location = '省内' if is_in_province else '省外'
        return price, f"{tier}-{location}"
    
    def get_corn_price(self, weight, province):
        weight = float(weight)
        province = str(province)
        
        if province in self.corn_prices['special']:
            special = self.corn_prices['special'][province]
            if weight <= 1:
                return special['base'], f"特殊地区-{province}"
            else:
                extra_weight = weight - 1
                total_price = special['base'] + special['continuation'] * extra_weight
                return total_price, f"特殊地区-{province}"
        
        tier = None
        for tier_name, tier_data in self.corn_prices.items():
            if tier_name != 'special' and province in tier_data['provinces']:
                tier = tier_data
                break
        
        if not tier:
            tier = self.corn_prices['tier3']
        
        if weight <= 0.5:
            return tier['prices']['0-0.5kg'], "0-0.5kg"
        elif weight <= 1:
            return tier['prices']['0.5-1kg'], "0.5-1kg"
        elif weight <= 2:
            return tier['prices']['1.01-2kg'], "1.01-2kg"
        elif weight <= 3:
            return tier['prices']['2.01-3kg'], "2.01-3kg"
        else:
            base_price = tier['prices']['3kg+']
            extra_weight = weight - 3
            total_price = base_price + tier['continuation_fee'] * extra_weight
            return total_price, f"3kg+续重({extra_weight:.2f}kg)"
    
    def process_data(self, data_file, product_type, fee_file=None):
        try:
            if data_file.endswith('.xlsx'):
                df = pd.read_excel(data_file)
            else:
                try:
                    df = pd.read_csv(data_file, encoding='utf-8')
                except:
                    df = pd.read_csv(data_file, encoding='gbk')
        except Exception as e:
            raise Exception(f"读取数据文件失败: {e}")
        
        additional_fees = {}
        if fee_file and os.path.exists(fee_file):
            try:
                if fee_file.endswith('.xlsx'):
                    fee_df = pd.read_excel(fee_file)
                else:
                    try:
                        fee_df = pd.read_csv(fee_file, encoding='utf-8')
                    except:
                        fee_df = pd.read_csv(fee_file, encoding='gbk')
                
                for _, row in fee_df.iterrows():
                    tracking_number = None
                    fee_amount = 0
                    
                    for col in fee_df.columns:
                        if '运单号' in col or 'tracking_number' in col:
                            tracking_number = str(row[col]).strip()
                        elif '结算金额' in col or 'settlement_amount' in col:
                            fee_amount = float(row[col]) if pd.notna(row[col]) else 0
                    
                    if tracking_number and tracking_number != 'nan':
                        additional_fees[tracking_number] = fee_amount
            except:
                pass
        
        results = []
        stats = {'total_orders': 0, 'total_basic_fee': 0, 'total_additional_fee': 0, 'total_fee': 0}
        
        for index, row in df.iterrows():
            try:
                weight = row.get('计算重量') or row.get('billing_weight', 0)
                province = row.get('收件人省份') or row.get('dest_province', '')
                tracking_number = str(row.get('tracking_number') or row.get('订单编号', f'订单{index+1}'))
                shop_name = row.get('店铺名称') or row.get('merchant_name', '')
                logistics_company = row.get('物流公司名称') or row.get('logistics_company', '')
                
                if len(tracking_number) < 10 or tracking_number == 'nan':
                    full_tracking_number = str(tracking_number).zfill(15)
                else:
                    full_tracking_number = tracking_number
                
                if product_type == '小麦':
                    basic_fee, tier_info = self.get_wheat_price(weight, province)
                elif product_type == '玉米':
                    basic_fee, tier_info = self.get_corn_price(weight, province)
                else:
                    basic_fee, tier_info = 0, '未知产品'
                
                additional_fee = additional_fees.get(tracking_number, 0)
                total_fee = basic_fee + additional_fee
                
                result = {
                    '序号': index + 1,
                    '快递单号': full_tracking_number,
                    '店铺名称': shop_name,
                    '物流公司': logistics_company,
                    '实际重量': f"{weight}kg",
                    '收件省份': province,
                    '产品类型': product_type,
                    '计费档位': tier_info,
                    '基础费用': basic_fee,
                    '附加费用': additional_fee,
                    '总费用': total_fee
                }
                
                results.append(result)
                stats['total_orders'] += 1
                stats['total_basic_fee'] += basic_fee
                stats['total_additional_fee'] += additional_fee
                stats['total_fee'] += total_fee
                
            except Exception as e:
                continue
        
        return results, stats

calculator = ExpressCalculator()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        product_type = request.form.get('product_type')
        
        if 'data_file' not in request.files:
            return jsonify({'error': '请选择数据文件'})
        
        data_file = request.files['data_file']
        if data_file.filename == '':
            return jsonify({'error': '请选择数据文件'})
        
        task_id = str(uuid.uuid4())
        data_filename = secure_filename(f"{task_id}_data_{data_file.filename}")
        data_path = os.path.join(app.config['UPLOAD_FOLDER'], data_filename)
        data_file.save(data_path)
        
        fee_path = None
        if 'fee_file' in request.files:
            fee_file = request.files['fee_file']
            if fee_file.filename != '':
                fee_filename = secure_filename(f"{task_id}_fee_{fee_file.filename}")
                fee_path = os.path.join(app.config['UPLOAD_FOLDER'], fee_filename)
                fee_file.save(fee_path)
        
        results, stats = calculator.process_data(data_path, product_type, fee_path)
        
        output_filename = f"快递费用核算结果_{product_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_path = os.path.join(app.config['RESULTS_FOLDER'], output_filename)
        
        df = pd.DataFrame(results)
        df['基础费用显示'] = df['基础费用'].apply(lambda x: f"{x:.2f}元")
        df['附加费用显示'] = df['附加费用'].apply(lambda x: f"{x:.2f}元")
        df['总费用显示'] = df['总费用'].apply(lambda x: f"{x:.2f}元")
        
        display_df = df[['序号', '快递单号', '店铺名称', '物流公司', '实际重量', '收件省份', '产品类型', '计费档位', '基础费用显示', '附加费用显示', '总费用显示']]
        display_df.columns = ['序号', '快递单号', '店铺名称', '物流公司', '实际重量', '收件省份', '产品类型', '计费档位', '基础费用', '附加费用', '总费用']
        
        summary_data = [
            ['项目', '数值'],
            ['订单总数', stats['total_orders']],
            ['基础费用总计', f"{stats['total_basic_fee']:.2f}元"],
            ['附加费用总计', f"{stats['total_additional_fee']:.2f}元"],
            ['总费用合计', f"{stats['total_fee']:.2f}元"],
            ['计算时间', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ['产品类型', product_type]
        ]
        summary_df = pd.DataFrame(summary_data)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='汇总统计', index=False, header=False)
            display_df.to_excel(writer, sheet_name='详细明细', index=False)
        
        try:
            os.remove(data_path)
            if fee_path:
                os.remove(fee_path)
        except:
            pass
        
        return jsonify({
            'success': True,
            'stats': stats,
            'download_url': f'/download/{output_filename}',
            'filename': output_filename
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/download/<filename>')
def download_file(filename):
    file_path = os.path.join(app.config['RESULTS_FOLDER'], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return "文件不存在", 404

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    
    print("=" * 60)
    print("    快递费用核算工具 (大数据版) 已启动")
    if 'PORT' in os.environ:
        print("    部署模式运行")
    else:
        print(f"    本地访问: http://127.0.0.1:{port}")
    print("=" * 60)
    
    app.run(debug=False, host='0.0.0.0', port=port)
