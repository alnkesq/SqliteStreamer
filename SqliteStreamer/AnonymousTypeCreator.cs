using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace SqliteStreamer;

public static class AnonymousTypeCreator
{
    public static Type CreateAnonymousType(IReadOnlyList<(string Name, Type? Type)> fields)
    {

        var asmnameStr = "AnonymousTypes_" + Guid.NewGuid();
        var asmname = new AssemblyName(asmnameStr);
        var asm = AssemblyBuilder.DefineDynamicAssembly(asmname, AssemblyBuilderAccess.Run);
        var module = asm.DefineDynamicModule(asmnameStr);
        var builder = module.DefineType("AnonymousObject", TypeAttributes.Public);

        var emptyconstr = builder.DefineConstructor(MethodAttributes.Public | MethodAttributes.HideBySig, CallingConventions.HasThis, Array.Empty<Type>());
        var il = emptyconstr.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Call, typeof(object).GetConstructor(Array.Empty<Type>())!);
        il.Emit(OpCodes.Ret);

        foreach (var field in fields)
        {
            var fieldType = field.Type ?? typeof(object);
            builder.DefineField(field.Name, fieldType, FieldAttributes.Public);
        }
        
        return builder.CreateTypeInfo();
    }
}